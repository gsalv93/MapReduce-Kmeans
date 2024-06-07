from job import MRKmeans, Point
import shutil
import math
import sys, os
import random
import ast
import copy
import logging

number_of_iterations = 10
threshold = 0.001
CENTROIDS_FILE = "temp/cent.txt"


def check_threshold(old_centroids, new_centroids, threshold):

    for i, old_point in enumerate(old_centroids):
        old_centroid_point = Point(old_point)
        dist = old_centroid_point.distance_points(new_centroids[i])
        print(dist)
        if(dist > threshold):
            #If the two points are not that close to eachother, I continue the kmeans algorithm
            #I check
            return False
    #Otherwise, I stop the algorithm as the two centroids are pretty close to eachother
    return True

def get_c_from_runner(job, runner):
    c = []    
    for line in runner.cat_output():
        list_of_values = line.decode("utf-8").split("\t")
        if(list_of_values[0] != ''):
            tmp = list_of_values[1].strip("\n")

            c.append(ast.literal_eval(tmp))

    return c


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

def write_c(centroid_list, file):
    with open(file,'w') as f:
        for centroid in centroid_list:
            listToStr = ','.join([str(c) for c in centroid])
            print(listToStr)
            f.write(listToStr)
            f.write("\n")

if __name__ == '__main__':
    old_centroids = []
    new_centroids = []
        
    old_centroids = get_c(CENTROIDS_FILE)
    mr_job = MRKmeans()
    for i in range(number_of_iterations):
        print(f"ITERATION {i}")
        with mr_job.make_runner() as runner:
            if runner.fs.local.exists("hi"):
                runner.fs.local.rm("hi")
            runner.log_level = 'INFO'
            runner.run()
            new_centroids = get_c_from_runner(mr_job,runner)
            #extracting new centroids
            write_c(new_centroids, CENTROIDS_FILE)

        if check_threshold(old_centroids, new_centroids, threshold):
            break
        else:
            old_centroids = new_centroids


    #write_c(new_centroids, "final_output.txt")