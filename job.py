#!/usr/bin/python
#coding:utf-8
from mrjob.job import MRJob
from mrjob.step import MRStep
from scipy.spatial import distance
import numpy as np
import shutil
import math
import sys, os
import random
import ast
import copy

class Point(object):
    coordinates = []
    def __init__(self, coordinates):
        self.coordinates = coordinates
    #Parse mapper line and obtain the coordinates
    def parse(self, line):
        self.coordinates = [float(num) for num in line.split(",")]

    #Calculate the distance between myself and the other point in input
    def distance_points(self, other_point):
         return distance.euclidean(self.coordinates, other_point)

class MRKmeans(MRJob):
    centroids = ""
    
    def configure_args(self):
        super(MRKmeans, self).configure_args()
        self.add_file_arg('--cf', help="Input centroids")

    def mapper_init(self):
        self.centroids = get_c(self.options.cf)

    def mapper(self, _, line):
        # Run k-means clustering
        point = Point([])
        point.parse(line)
        
        #I skip the numbers present in the starting centroids
        min_distance = math.inf
        distance = 0.0
        nearest = -1
        i = 0
        #Find the closest centroid to my points
        for centroid in self.centroids:
                
            distance = point.distance_points(centroid)
            if distance < min_distance:
                min_distance = distance
                nearest = i
            i += 1
        
        # Emit nearest centroids and corresponding data points
        yield nearest, point.coordinates
      
    def reducer(self, key, values):
        list_of_points = list(values)
        length = len(list_of_points)
        partial_sum = list_of_points[0]
        for i in range(1, len(list_of_points)):
            point = list_of_points[i]
            for j in range(len(point)):
                partial_sum[j] += point[j]

        mean_centroid = []
        for i in range(len(partial_sum)):
            mean_centroid.append(partial_sum[i] / length)

        
        yield 42, (list(mean_centroid), key)

    def reducer_sort(self, _, points_with_key_value):
        # Sort each centroid by their ID
        initial_centroids = sorted(points_with_key_value, key=lambda x: x[1])
        
        # Output new centroids
        for centroid in initial_centroids:
            yield centroid[1], centroid[0]
    
    
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                        mapper=self.mapper,
                        reducer=self.reducer),
                MRStep(reducer=self.reducer_sort)]



def get_c(file):
    centroids = []
    with open(file, 'r') as f:        
        for line in f:
            if line.startswith("["):
                list_of_values = ast.literal_eval(line)
                centroids.append(list_of_values)
            else:
                x, y, z, w, k, l, o = line.split(',')
                centroids.append([float(x), float(y), float(z), float(w), float(k), float(l), float(o)])
    
    return centroids


if __name__ == '__main__':
    MRKmeans.run()