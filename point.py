from scipy.spatial import distance
import numpy as np
import sys

class Point(object):
    coordinates = []
    
    #Class constructor
    def __init__(self, coordinates):
        self.coordinates = coordinates
    
    #Parse mapper line and obtain the coordinates
    def parse(self, line):
        self.coordinates = np.array([float(x) for x in line.split(',')])

    #Calculate the distance between myself and the other point in input
    def distance_points(self, other_point):
        return distance.euclidean(self.coordinates, other_point)
    