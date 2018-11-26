
'''
    此类事停车位的类，属性为停车位的位置，停车位的等级(分等级停车位的单价不一样)，停车位编号，停车位状态
    方法为属性需要的get，set方法
'''
class ParkingSpace():

    def __init__(self,location = "default",garde = "default",id = "default",state = True):
        self.__location = location
        self.__garde = garde  
        self.__id = id
        self.__state = state

    def set_state(self,state):
        self.__state = state 

    def get_location(self):
        return self.__location

    def get_garde(self):
        return self.__garde   

    def get_id(self):
        return self.__id   