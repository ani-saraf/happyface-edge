from picamera import PiCamera
from time import sleep
from os import path, listdir, remove, mkdir
from tempfile import gettempdir
from datetime import datetime
from process_image_edge import process_image

image_folder = '/home/pi/Pictures'
temp_dir = "{}/happyface".format(gettempdir())

if not path.exists(temp_dir):
    mkdir(temp_dir)

def capture_image(folder_name):
    cam = PiCamera()
    cam.resolution = (2592, 1944)
    for count in range(1):
        sleep(2)
        filename = 'Image_{}.jpg'.format(datetime.now().strftime('%Y%m%d_%H%M%S'))
        image_path = path.join(temp_dir, filename)
        cam.capture(image_path)
        print "Captured {}".format(image_path)

print "\nStarted capturing images ..."
capture_image(image_folder)
print "Completed capturing images."

print "\nStarted processing images ..."
for img_file in listdir(temp_dir):
    tmp_file = path.join(temp_dir, img_file)
    process_image(tmp_file)
    remove(tmp_file)
    print "Processed {}".format(tmp_file)
print "Completed processing images."

