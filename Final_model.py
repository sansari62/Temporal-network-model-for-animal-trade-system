# -*- coding: utf-8 -*-
"""
Created on Fri Jul  9 10:38:34 2021

@author: ansari
"""
"""
The following program implements the algorithm for temporal network model mentioned in 
the paper "A Temporal Model for Animal Trade System". The model is stochastic and simulates
the trades between farms based on German pig trade dataset. 

"""


import random
from scipy.stats import  powerlaw, expon, lognorm, beta
import math
import time
from numpy import random as np
import pdb

#**********************************************************
        #Model Parameters 

#*********************************** 1*********************

random.seed(10)
output=open("syndata.txt","w+")                             #output file store the generated dataset
S = range(4)                                                # list of stages in production chain
S_label = [ 'breeding', 'fattening','trader', 'slaughter']  # or whatever 'piglet production',
                                  
# ns keeps number of barns of every type/stage [B,F,T,S] 
# for generating a network of 100 farms:ns should be:[60,48,5,7], 
#for creating a network with 10000 farms:[5000, 4000, 400, 600] 
ns = [26000,24000,3590,4000]      
              
theta = [lambda: 0.65 * lognorm.rvs(s = 1.88, scale  = math.exp(4.27)),        
        lambda:  1.6 * lognorm.rvs(s = 2.07, scale  = math.exp(3.74)),      
        lambda:  0.3 * lognorm.rvs(s = 2.85, scale  = math.exp(6.66)),        
        lambda:  500 * lognorm.rvs(s = 1.53, scale = math.exp(-0.11))]      

birth_rate =  [ 0.037, 0, 0, 0]      #this rate used in poisson distribution in the code to produce no.newborn per day
mortal_rate = [ 0.0042, 0.0008, 0.000, 1] 
min_bch_size = [lambda: lognorm.rvs(s=1.44,scale=math.exp(3.23)),
                lambda: lognorm.rvs(s=0.89,scale=math.exp(4.07)),
                lambda: expon.rvs(scale =1/0.0078 ) ]
                
                
loyalty = [lambda: beta.rvs(73.50440921808314, 1.2781792349749783, -3.3232837823529495, 4.322420258008411),
           lambda: beta.rvs(5,1.3),
           lambda: powerlaw.rvs(57847145.245008886, -5412343.7220429, 5412344.720996874)]   
T = [(0,1,80),(1,2,100),(2,3,0)]     # T is a set of possible transactions
barnlist = []                        # keep barns   
l = [0] * ns[0]
barn_index = {}                      # dict of pairs (start_index, end_index) giving the index ranges for barns of each type

#******************************* Barn Class **********************************************
#Each animal holding is an object or instance of class barn

#******************************************************************************************

class Barn:
    """
    describe the attributes and methods for each barn
    """

    def __init__(self, Barn_id, stage_type, capacity,loyal, gis, Dlist):
        """
        initializing each barn with attributes like id,type and capacity
        Dlist shows a queue inside each barn
        each barn has a gis for keeping gis (the id of last barn which sent a batch )
        """

        self.Barn_id = Barn_id
        self.stage_type = stage_type
        self.capacity = capacity         #farm size
        self.Dlist = Dlist               #includes queues in barn
        self.gis = gis                   #gis is the id of last partner
        if self.stage_type <= 2:
            self.loyal = loyal



    def create_Dlist(self):
        """
        create a zero Dlist (empty queues inside barns) at first for every successor stage in every barn
        based on barn type
        T is a set of successor stages for current stage s
        """
        #for gis choose randomly one from all barns in the next stage as default gis for each queue in
        #current barn
        self.capacity = int((theta[self.stage_type]()))     #generate farm size from a distribution defied above
        stage = self.stage_type
        #the min capacity for all farms except slaughters are 30 and for slaughters is 100,
        #the max capacity is limited to 10000
        if (self.capacity)> 10000:
                        self.capacity = int((theta[self.stage_type]()))
                        while (self.capacity)> 10000:
                             self.capacity = int((theta[self.stage_type]())) 
        if self.capacity <  30:
                if stage < 3:
                  self.capacity = 30
                else:
                  self.capacity = 100  
        #print("\n" ,self.capacity)
        if stage == 0:
            # only for breeding barns at t=0 add animals
            
            A = birth_rate[self.stage_type] * self.capacity
            p = 1 / (1 + A)          # the parameter p in the geometeic distrib.
            #print("the value is:", 1-p)
            l[self.Barn_id] = math.log(1 - p)
            

        if stage < 2:
            #initialization for breeding and fattening farms
            self.gis[stage+1] =  random.choice(range(barn_index[stage+1][0],barn_index[stage+1][1]+1))
            self.gis[stage+2] =  random.choice(range(barn_index[stage+2][0],barn_index[stage+2][1]+1))
            self.Dlist[0] = []
            self.loyal  = loyalty[self.stage_type]()    #assign loyalty to each barn

        else:
            #traders have 2 queues
            if stage == 2:    #initialization for trader farm
                     self.gis[1] = random.choice(range(barn_index[1][0],barn_index[1][1]+1))
                     self.gis[3] = random.choice(range(barn_index[3][0],barn_index[3][1]+1))
                     self.Dlist[1] = []
                     self.Dlist[3] = []
                     self.loyal  = loyalty[self.stage_type]()    #assign loyalty to each barn
            else:
                self.Dlist[0] = []    #initialization for slauhter farm


    def compute_free_capacity(self):
        """
        compute the current free size of barn j
        """
        return self.capacity -  sum([len(self.Dlist[d]) for d in self.Dlist])


    def update_after_transition(self, queue,next_stage, j, x):
        """
        update Dlist of the current barn after moving a batch of
        animal from barn i to barn j,remove x enteries from queue in
        the current barn
        """
        self.Dlist[queue] = self.Dlist[queue][x:]
        self.gis[next_stage] = j.Barn_id  #set the gis t0 the last trade partner for farm i 
        #if moving pigs to traders check for sender:
        if next_stage == 2:
            #for pigs sent by breeding farms put them in the first queue of trader
            if self.stage_type== 0:
                for xi in range(x):
                    j.Dlist[1].append(0)
            else:
                #for those pigs sent by fattening farms put them in the second queue of trader
                for xi in range(x):
                    j.Dlist[3].append(0)
        else:
            #for sending to other types just put them in their queue
            for xi in range(x):
                j.Dlist[0].append(0)



    def transfertoj(self, queue, next_stage, x, time, q):
        """
        moving x animals from barn i to barn j  at time t
        """
        y=0
        if random.random() < self.loyal:
            if barnlist[self.gis[next_stage]].compute_free_capacity() >= x:
            # send all x to last destination due to loyalty:
                j = barnlist[self.gis[next_stage]]
                y = x
               
            else:
                if barnlist[self.gis[next_stage]].compute_free_capacity() >= q:
                   j = barnlist[self.gis[next_stage]]
                   y = j.compute_free_capacity()
                                     
        else:
            # check if any potential destination has free capacity >= x:
            potential_destinations = barnlist[barn_index[next_stage][0]:barn_index[next_stage][1]+1]
            potential_js = [i for i in potential_destinations if i.compute_free_capacity() >= x]
            if len(potential_js) > 0:
                # send all x to a random destination with enough free capacity:
                j = random.choice(potential_js)
                y = x
               
            else:
                # check if any potential destination has free capacity >= qss':
                potential_js = [i for i in potential_destinations if i.compute_free_capacity() >= q]
                if len(potential_js) > 0:
                    # send all x to a random destination with enough free capacity:
                    j = random.choice(potential_js)
                    y = j.compute_free_capacity()
                    assert q <= y < x
                    
                else:
                    y = 0
                    

        if y > 0 and time >=730 :
            output.write(str(self.Barn_id) + "," + str(j.Barn_id) + "," + str(time - 730) + "," + str(y) + "\n")
            self.update_after_transition(queue, next_stage, j, y)
            
          # else:
            #pass
           # print("no transfer is possible from barn",self.Barn_id," at time: ",time)

    def compute_X(self, queue):
        """
        compute the value x of animals with the age>dss'in queue i of current barn
        """
        return len([age for age in self.Dlist[queue] if age >= T[self.stage_type][2]])


    def add_newborn(self):
        """
        add k newborn to queue i of current barn
        """
        k = 0
        free_capacity = self.compute_free_capacity()
        if(free_capacity > 0):
            
                k0 = int(math.log(np.rand()) / l[self.Barn_id])
                k = min( k0 , free_capacity )
                self.Dlist[0] = self.Dlist[0] + [0] * k           #add k newborn to queue i

    def die_animal(self, queue):
         """
         removing animals from queues with ms probability
         """
         for animal in self.Dlist[queue]:             #for every animal in  queue i of the current barn
             if random.random() < mortal_rate[self.stage_type]:
                self.Dlist[queue].remove(animal)


    def process_barn(self, time):
        """
        process current barn(just for breeding and fattening), means process all queue inside
        """
         #some pigs may die and other remaining's age increased by 1
        queue = 0           # only one queue is available in breeding and fatteing farms
        self.die_animal(queue)
        self.Dlist[queue] = [age + 1 for age in self.Dlist[queue]]
        if(self.stage_type == 0):    # only in breeding farms new pigs are born 
           self.add_newborn()
           
        q = int(min_bch_size[self.stage_type]())      # determine the min batch size based on farm type
        if len(self.Dlist[queue]) > q :
            #check to see if size of current queue is at least as q
                if self.Dlist[queue][q - 1] >= T[self.stage_type][2] :
                    #check if in barni at stage s reached to minimum batch size
                    x = self.compute_X(queue)  # compute batch_size to move
                    if self.stage_type == 0:
                        # breeding farm with 60% probabality send to trader and with the 40%
                        # probability send to fatttening
                        if random.random()< 0.4 :
                            next_stage = 1
                        else:
                            next_stage = 2
                    else:
                        #for fattening barns with 30% probability send to slaughter
                        #and  with 70% probablity send to trader
                        if random.random() < 0.3:
                            next_stage = 3
                        else:
                            next_stage = 2
                    self.transfertoj(queue,next_stage, x, time, q)

#*************************************************End of class*******************************

def proceed_over_time(time_limit):
    """
    process all barns in a randomly order over time
    """
    index_list = []
    for t in range(1, time_limit + 1):
                  
        print("t =", t)
        index_list = random.sample(range(0, len(barnlist)), len(barnlist))
        for i in index_list:                    #process barnlist in a random order
            if barnlist[i].stage_type == 3:     # for slaughters only empty the queue
               barnlist[i].Dlist[0] = []
            else:
                #for trader barns, check possible animal movements
                if barnlist[i].stage_type == 2:
                    for queue in barnlist[i].Dlist :
                        if len(barnlist[i].Dlist[queue])>0:
                            barnlist[i].die_animal(queue)
                            q = int(min_bch_size[barnlist[i].stage_type]())
                            next_stage = queue
                            barnlist[i].transfertoj(queue, next_stage, len(barnlist[i].Dlist[queue]),t,q)
                else:
                    # for Breeding and Fattenig barns
                    barnlist[i].process_barn(t)

def compute_indexRange():
    """
    compute ranges for accessing list of barns in every stage
    """
    end_index = 0
    for i in S:
        if i == 0:
            start_index = 0
        else:
            start_index = end_index + 1
        end_index = start_index +ns[i] - 1
        barn_index[i] = (start_index, end_index)
        
        
        
def  init_barns(blist):
    
    
    
     Bid = 0           #barn ID
     barnlist = blist
     for stage in S:
         for j in range(ns[stage]):             #create ns Barn for each stage in S
             barnlist.append(Barn(Bid, stage,
                                     0,0,
                                       {}, {}))
             Bid += 1
     compute_indexRange()                        #compute the range of barn index for every stage
    
    # process all barns at time 0: create empty queues inside every barn
     for i in range(len(barnlist)):
        barnlist[i].create_Dlist()
        
    # set 2% of fattening farm loyalty randomly to 0 and also 1% of traders loyalty to 0 
     f_index  = random.sample(range(ns[0],ns[0]+ns[1]),k = int(0.02 * ns[1]))
     t_index  = random.sample(range(ns[0]+ns[1],ns[0]+ns[1]+ns[2]),k = int(0.01 * ns[2]))
     for farm in f_index:
        barnlist[farm].loyal = 0
     for farm in t_index:
        barnlist[farm].loyal = 0    
    
    # check for congestion
     capacityB = sum([barn.capacity for barn in barnlist[barn_index[0][0]:barn_index [0][1]+1]] )
     capacityF = sum([barn.capacity for barn in barnlist[barn_index[1][0]:barn_index [1][1]+1]] )
     capacityT = sum( [barn.capacity for barn in barnlist[barn_index[2][0]:barn_index [2][1]+1]] )
     capacityS = sum([barn.capacity for barn in barnlist[barn_index[3][0]:barn_index [3][1]+1]] )
     print (capacityB," ",capacityF," ",capacityT, " ",capacityS," ")
     if capacityB <= capacityF :
         flag = True
     else:                    # if congestion happens init barns again with new capecities until no congestion
         flag = False
     if (not flag):
         init_barns([])       #empty previous barn list because of the capacity problem and again initialize barns
         

###########################################THE MAIN SECTION START HERE###########################################

def main():
             
    # initialize data and create barns:
    
    capacity_file = open("barn_size.txt","w+")  # capacity file keep size of barns
    start = time.time()
    init_barns(barnlist) 
     
        
    for i in range(len(barnlist)):
        capacity_file.write(str(barnlist[i].capacity )+",")
    capacity_file.close()
     
    time_limit = 2190     # observation period set to 6 years, autmatically the first 2-years data removed
    proceed_over_time(time_limit) 
    output.close()
    finish = time.time() 
    print("the execution time is: ",finish - start)
if __name__ == "__main__":
    main()

