#!/usr/bin/python

# Copyright 2016 Shakir James. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================

"""Loader for Wikipedia image search

Load two key/value stores: images and terms. The images KVS maps
the DBpedia/Wikipedia category (key) to the image URLs (value),
and the terms KVS maps the a label (key) to a DBpedia categories.

To run:
$ python loader.py -d --filter=Al
"""
from __future__ import print_function

import logging

from argparse import ArgumentParser, FileType
from .kvs import Dict, DynamoDB, Shelf
from .parser_a import image_iterator, label_iterator, Stemmer

IMAGES_KVS_NAME = 'images'
TERMS_KVS_NAME = 'terms'
STORAGE_CHOICES = {
    'disk': Shelf,
    'mem': Dict,
    'cloud': DynamoDB
}


def parse_args(prog='loader', description='Wiki loader.'):
    parser = ArgumentParser(prog=prog, description=description)
    parser.add_argument('-d', dest='debug', action='store_true')
    parser.add_argument('--filter', default='')
    parser.add_argument(
        '--kvs', choices=STORAGE_CHOICES.keys(), default='disk')
    parser.add_argument(
        '--images', type=FileType('r'), default='./data/images_en.nt')
    parser.add_argument(
        '--labels', type=FileType('r'), default='./data/labels_en.nt')
    return parser.parse_args()


def load_images(kvs, image_iterator):
  
	logging.debug('load_images{}'.format(kvs))

	for category, url in image_iterator:
		logging.debug('image {} => {}'.format(category, url))
		kvs.put( category, url)

		

def load_terms(kvs, images_kvs, label_iterator):

    stemmer = Stemmer()
    logging.debug('load_terms {}'.format(kvs))
    for category, label in label_iterator:
        if category not in images_kvs:  # TODO
            continue
        words = label.split()
        for word in words:
            stemmed = stemmer.stem(word)  # FIXME
            kvs.put(stemmed, category)  # TODO
            logging.debug('label {} ({}) => {}'.format(
                word, stemmed, category))
	
def main():
    args = parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    logging.debug('debug {}'.format(args.debug))
    logging.debug('images {}'.format(args.images))
    logging.debug('labels {}'.format(args.labels))
    logging.debug('filter {}'.format(args.filter))
    logging.debug('kvs {}'.format(args.kvs))

    images = image_iterator(args.images, filter=args.filter)
    images_kvs = STORAGE_CHOICES[args.kvs](IMAGES_KVS_NAME)
    load_images(images_kvs, images)

    terms_kvs = STORAGE_CHOICES[args.kvs](TERMS_KVS_NAME)
    labels = label_iterator(args.labels)
    load_terms(terms_kvs, images_kvs, labels)

    terms_kvs.close()
    images_kvs.close()

if __name__ == '__main__':
    main()
