# -*- coding: utf-8 -*-
import os, subprocess
import cv2
import numpy as np
import tempfile
from pdf2image import convert_from_path
import json
from tqdm import tqdm
import base64
from wand.api import library
from wand.image import Image

flow = 'RC_SCAN'
folder = '34'
input_path = '/Users/momo/Google Drive/My Drive/Project/AIA Project/AIA_document/Document/UAT/Test Case/{0}/{1}'.format(flow,folder)
output_folder = '/Users/momo/Google Drive/My Drive/Project/AIA Project/AIA_document/Document/UAT/Test Case/{0}/{1}/TIFF/'.format(flow,folder)
base64_folder = '/Users/momo/Google Drive/My Drive/Project/AIA Project/AIA_document/Document/UAT/Test Case/{0}/{1}/base64/'.format(flow,folder)

dpi = 300

pdf_lists = sorted([file for file in os.listdir(input_path) if file.endswith(".pdf")])
if not os.path.exists(output_folder):
    os.mkdir(output_folder)
if not os.path.exists(base64_folder):
    os.mkdir(base64_folder)
for f in tqdm(pdf_lists):
    filename = os.path.join(input_path,f)
    print(filename)
    jpg_filename = filename.split('/')[-1]
    tiff_filename = jpg_filename.replace(jpg_filename.split('.')[-1],'tif')
    print(tiff_filename)
    save_path = output_folder+tiff_filename
    print(save_path)
    with Image(filename=filename, resolution=300) as img:
        img.type = 'bilevel'
        img.compression = "group4"
        # Manually iterate over all page, and turn off alpha channel.
        library.MagickResetIterator(img.wand)
        for idx in range(library.MagickGetNumberImages(img.wand)):
            library.MagickSetIteratorIndex(img.wand, idx)
            img.alpha_channel = 'off'
        img.save(filename=save_path)
    ## convert to base64
    with open(filename, "rb") as image_file:
        encoded_bytes = base64.b64encode(image_file.read())
        text_base64 = encoded_bytes.decode()
        # print(encoded_string)
    txt_filename = jpg_filename.replace(jpg_filename.split('.')[-1],'txt')
    base64_path = base64_folder+txt_filename
    with open(base64_path, "w") as text_file:
        text_file.write((text_base64))