from parking_record import ParkingRecord
from parking_space import ParkingSpace
from parking_order import ParkingOrder
from car import Car

'''
    此类是停车场类，属性有停车位的列表，可用的停车位列表下标，剩余停车位数量
    方法具体如下
'''
class Depot():
    parking_space = [ParkingSpace("一楼",1,0),ParkingSpace("一楼",1,1),ParkingSpace("一楼",1,2),
    ParkingSpace("二楼",2,3),ParkingSpace("二楼",2,4),ParkingSpace("二楼",2,5),
    ParkingSpace("三楼",3,6),ParkingSpace("三楼",3,7),ParkingSpace("三楼",3,8)]

    parking_space_empty = 0
    parking_space_enable = len(parking_space)

    #新增停车记录属性
    def set_record(self,record):
        self.__record = record

    #新增订单属性
    def set_order(self,order):
        self.__order = order

    #记录车进入停车场的信息（记录车，时间，停车位状态修改，可用停车位与剩余停车位修改）
    def notes_in_depot(self,car = Car(),in_time = "default"):
        self.set_record(ParkingRecord(car = car))
        self.__record.set_in_time(in_time)
        space = Depot.parking_space[Depot.parking_space_empty]
        space.set_state(False)
        self.__record.set_parking_space(space)
        Depot.parking_space_empty += 1
        Depot.parking_space_enable -=1

    #记录进入停车位的信息（时间）
    def notes_in_parking_space(self,in_parking_time = "default"):
        self.__record.set_in_packing_time(in_parking_time)
    
    #记录离开停车位的信息（时间）
    def notes_out_parking_space(self,out_parking_time = "default"):
        self.__record.set_out_packing_time(out_parking_time)

    #记录离开停车场的信息（记录时间，停车位状态修改，可用停车位与剩余停车位修改）
    def notes_out_depot(self,out_time = "default"):
        self.__record.set_out_time(out_time)
        self.set_order(ParkingOrder(parking_record = self.__record))
        space = Depot.parking_space[Depot.parking_space_empty]
        space.set_state(True)
        Depot.parking_space_empty -= 1
        Depot.parking_space_enable +=1

    #生成订单信息
    def make_order(self):
        self.__order.print_order()        




        




       
    