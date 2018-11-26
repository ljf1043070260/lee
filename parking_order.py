from parking_record import ParkingRecord
import time

'''
    此类为停车订单类，属性有所处理的停车记录，应缴费金额
    方法有根据停车记录算得缴费金额，生成停车订单方法
'''
class ParkingOrder():
    def __init__(self,parking_record = ParkingRecord(),money = 0):
        self.__parking_record = parking_record
        self.money = money 

    def calculate_money(self):
        garde = self.__parking_record.get_parking_space().get_garde()
        in_time = time.mktime(time.strptime(self.__parking_record.get_in_time(),"%Y-%m-%d %H:%M:%S")) 
        out_time = time.mktime(time.strptime(self.__parking_record.get_out_time(),"%Y-%m-%d %H:%M:%S")) 
        paring_time = (out_time - in_time)//60//60
        if garde == 1:
            return 20 * paring_time
        elif garde == 2:
            return 40 * paring_time
        elif garde == 3:
            return 60 * paring_time
    def print_order(self):
        print("-----------------------------")
        print("订单：")
        self.__parking_record.print_record()
        print("应付："+str(self.calculate_money())+"元")
        print("-----------------------------")

            



        
