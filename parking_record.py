from car import Car
from parking_space import ParkingSpace

'''
    此类为停车记录，属性为停的车，进入停车场的时间，进入停车位的时间，离开停车位的时间，离开停车场的时间，停的停车位
    方法为属性需要的get，set方法，还有一个生成停车记录方法，

'''
class ParkingRecord():   
    def __init__(self,car = Car(),parking_space = ParkingSpace(),in_time = "default",packing_time = "default",
        out_time = "default",out_packing_time = "default"):
        self.__in_time = in_time
        self.__in_packing_time = packing_time
        self.__out_packing_time = out_packing_time
        self.__out_time = out_time
        self.__car = car 
        self.__parking_space = parking_space

    def set_car(self,car):
        self.__car = car 

    def set_parking_space(self,parking_space):
        self.__parking_space = parking_space

    def set_in_time(self,in_time):
        self.__in_time = in_time  

    def set_in_packing_time(self,packing_time):
        self.__in_packing_time = packing_time 

    def set_out_packing_time(self,out_packing_time):
        self.__out_packing_time = out_packing_time 

    def set_out_time(self,out_time):
        self.__out_time = out_time   

    def get_car(self):
        return self.__car 

    def get_parking_space(self):
        return self.__parking_space

    def get_in_time(self):
        return self.__in_time  

    def get_in_packing_time(self):
        return self.__in_packing_time 

    def get_out_packing_time(self):
        return self.__out_packing_time 

    def get_out_time(self):
        return self.__out_time       

    def print_record(self):
        print("车主："+str(self.__car.get_id()))
        print("曾使用"+str(self.get_parking_space().get_id())+"号停车位")
        print("进入停车场时间："+str(self.__in_time))
        print("进入停车位时间："+str(self.__in_packing_time))
        print("离开停车位时间："+str(self.__out_packing_time))
        print("离开停车场时间："+str(self.__out_time))
    
   


                           




