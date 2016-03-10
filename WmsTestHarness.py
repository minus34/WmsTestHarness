# *********************************************************************************************************************
# PURPOSE: Uses multiprocessing to make concurrent WMS requests in separate processes for load testing a WMS service.
#          Supports the use of WMS as a tile map service (such as GeoWebCache services)
#
# USAGE: Edit the parameters before main() to set:
#     1. The proxy server (if required)
#     2. The number of requests and concurrent processes to run
#     3. The WMS service parameters
#     4. The min/max map width and bounding box(es). These limit the randomised map extents to the area(s) of interest
#
# TO DO:
#     1. Add support for WMTS services
#     2. Improve logging to record request failures (failures currently printed to screen only)
#
# NOTE: The log file will only be output after all test cases have been run, this is to avoid writing to a shared
#       log file from multiple processes (possible in Python, but complicated...)
#
# LICENSE: Creative Commons Attribution 3.0 Unported (CC BY 3.0)
# *********************************************************************************************************************

import csv
import math
import os
import random
import datetime
import urllib
import urllib2
import traceback
import multiprocessing

# # Set proxy for web requests (if required)
# proxy = urllib2.ProxyHandler({'http': '123.45.678.90'})
# opener = urllib2.build_opener(proxy)
# urllib2.install_opener(opener)

# Total number of WMS GetMap requests
requests = 100

# Number of concurrent processes to run
processes = 8

# Map tiles? (i.e. 256 x 256 pixel images in a Google/Bing Maps grid?)
map_tiles = True

# Map tile levels (only required if map_tiles = True)
min_tile_level = 11
max_tile_level = 18

# Map width limits in SRS units - allows random zoom scales to be tested (only required if map_tiles = False)
min_map_width = 100.0
max_map_width = 10000.0

# Map image width and height in pixels (only required if map_tiles = False)
map_image_width = 1024
map_image_height = 768

# WMS GetMap Request parameters
wms_server = "http://localhost:8080/geoserver/cite/service/wms"  # Sample WMS tile map service
# wms_server = "http://localhost:8080/geoserver/wms"  # Sample WMS service
layers = "a_map_Layer"
styles = ""
image_format = "image/png"
version = "1.3.0"  # Only v1.1.1 and v1.3.0 tested
srs = "EPSG:900913"  # web mercator

# Dictionary of max and min coordinates in SRS units. Used to randomly set map extents
max_bounding_boxes = {1: [16796997.0, -4020748.0, 16835959.0, -3995282.0],  # Sydney
                      2: [16124628.0, -4559667.0, 16163590.0, -4534318.0],  # Melbourne
                      3: [17021863.0, -3192356.0, 17048580.0, -3174789.0],  # Brisbane
                      4: [15417749.0, -4162522.0, 15447805.0, -4143515.0],  # Adelaide
                      5: [12884117.0, -3773816.0, 12921966.0, -3748880.0],  # Perth
                      6: [16391795.0, -5296763.0, 16410719.0, -5284614.0],  # Hobart
                      7: [16587717.0, -4225203.0, 16609981.0, -4187007.0]}  # Canberra

# max_bounding_boxes = {1: [-8243538.0, 4964586.0, -8225729.0, 4995275.0]}  # Manhattan


def main():
    print "Start WMS Stress Test\n"

    # Set default map tile set parameters
    if map_tiles:
        global map_image_width
        global map_image_height
        map_image_width = 256
        map_image_height = 256
        map_ratio = 1.0
    else:
        map_ratio = float(map_image_height) / float(map_image_width)

    # Count of bounding boxes to map within
    max_bounding_box_count = len(max_bounding_boxes)

    # Create pool of processes to get map images/tiles
    pool = multiprocessing.Pool(processes)

    # Create a shared memory list to combine results from all processes
    manager = multiprocessing.Manager()
    results_list = manager.list()

    start_time = datetime.datetime.now()

    # Set random map extents and fire off WMS requests in separate processes
    for i in range(0, requests):
        # Get random max/min bounding box
        max_bbox_num = random.randint(1, max_bounding_box_count)
        max_bbox = max_bounding_boxes.get(max_bbox_num)

        # Get random map width and height in SRS units
        if map_tiles:
            tile_level = random.randint(min_tile_level, max_tile_level)
            map_width = 256.0 * tile_pixel_sizes[tile_level]
            map_height = map_width
        else:
            map_width = random.uniform(float(min_map_width), float(max_map_width))
            map_height = map_width * map_ratio

        # Calculate random bottom/left map coordinates
        left = random.uniform(float(max_bbox[0]), float(max_bbox[2]) - map_width)
        bottom = random.uniform(float(max_bbox[1]), float(max_bbox[3]) - map_height)

        # Adjust bottom/left map coordinates to the Google/Bing Maps tile grid if creating map tiles
        if map_tiles:
            left = math.floor(left / map_width) * map_width
            bottom = math.floor(bottom / map_height) * map_height

        # Get top/right map coordinates
        right = left + map_width
        top = bottom + map_height

        # Construct WMS GetMap URL
        url_params = {"LAYERS": layers, "FORMAT": image_format, "SERVICE": "WMS",
                      "VERSION": version, "REQUEST": "GetMap", "STYLES": styles,
                      "SRS": srs, "WIDTH": str(map_image_width), "HEIGHT": str(map_image_height),
                      "BBOX": ''.join([str(left), ",", str(bottom), ",", str(right), ",", str(top)])}

        map_url = ''.join([wms_server, "?", urllib.urlencode(url_params)])

        # Fire off map request
        pool.apply_async(get_map, (map_url, results_list))

    pool.close()
    pool.join()

    elapsed_time = datetime.datetime.now() - start_time

    # Finish by logging parameters used and the results
    log_entries = list()

    # Title, parameters used and results summary
    log_entries.append(["WMS Stress Test Results"])
    log_entries.append([])
    log_entries.append(["Concurrent processes", processes])
    log_entries.append(["Map image size", str(map_image_width) + " x " + str(map_image_height), "pixels"])
    log_entries.append([])

    success_count = 0
    total_seconds = 0.0

    # Calculate some stats
    for item in results_list:
        seconds = item[0]
        image_size = item[1]

        if image_size > 0:
            success_count += 1
            total_seconds += seconds

    if success_count > 0:
        avg_seconds = total_seconds / float(success_count)
    else:
        avg_seconds = 0

    log_entries.append(["WMS requests", requests])
    log_entries.append([])
    log_entries.append(["Successful requests", success_count])
    log_entries.append(["Average time", avg_seconds, "seconds"])
    log_entries.append(["Failed requests", requests - success_count])
    log_entries.append([])
    log_entries.append(["Time_seconds", "Image_bytes", "URL"])

    # Output results to log file
    log_file = open(time_stamped_file_name(os.path.abspath(__file__).replace(".py", "")) + ".csv", 'wb')
    log_writer = csv.writer(log_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    log_writer.writerows(log_entries)
    log_writer.writerows(results_list)
    log_file.close()

    print "Finished: elapsed time = ", str(elapsed_time)


# Gets a map image and returns the time taken (seconds), image size (bytes) and the URL for logging
def get_map(url, mp_list):
    start_time = datetime.datetime.now()
    image_len = 0

    try:
        # Request map image and get its size as evidence of success or failure for logging
        request = urllib2.Request(url)
        image = urllib2.urlopen(request).read()
        image_len = len(image)
    except urllib2.URLError:
        # Print failures to screen (these aren't logged)
        print ''.join(["MAP REQUEST FAILED : ", url,  '\n', traceback.format_exc()])

    elapsed_time = datetime.datetime.now() - start_time
    elapsed_seconds = float(elapsed_time.microseconds) / 1000000.0

    mp_list.append([elapsed_seconds, image_len, url])


# Default Google/Bing map tile scales per level (metres per pixel)
tile_pixel_sizes = [156543.033906250000000000,
                    78271.516953125000000000,
                    39135.758476562500000000,
                    19567.879238281200000000,
                    9783.939619140620000000,
                    4891.969809570310000000,
                    2445.984904785160000000,
                    1222.992452392580000000,
                    611.496226196289000000,
                    305.748113098145000000,
                    152.874056549072000000,
                    76.437028274536100000,
                    38.218514137268100000,
                    19.109257068634000000,
                    9.554628534317020000,
                    4.777314267158510000,
                    2.388657133579250000,
                    1.194328566789630000,
                    0.597164283394814000,
                    0.298582141697407000,
                    0.149291070848703000,
                    0.074645535424351700,
                    0.037322767712175800,
                    0.018661383856087900,
                    0.009330691928043960,
                    0.004665345964021980,
                    0.002332672982010990,
                    0.001166336491005500,
                    0.000583168245502748,
                    0.000291584122751374,
                    0.000145792061375687]


# Adds a time stamp to a file name
def time_stamped_file_name(file_name, fmt='{file_name}_%Y_%m_%d_%H_%M_%S'):
    return datetime.datetime.now().strftime(fmt).format(file_name=file_name)


if __name__ == '__main__':
    main()
