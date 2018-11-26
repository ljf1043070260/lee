from depot import Depot
from car import Car
from car_user import CarUser
from parking_order import ParkingOrder
from parking_record import ParkingRecord
from parking_space import ParkingSpace




person001 = CarUser(name="user001",info="这是车主user001",car=Car("辽A888888","user001","宝马"))
person001.set_carlist(Car("辽B666666","user001","奔驰"))
car001 = person001.get_car_byid("辽A888888")
car002 = person001.get_car_byid("辽B666666")




#开始模拟流程
print("start")

depot = Depot()
#进入停车场
depot.notes_in_depot(car002,"2018-11-26 10:00:00")
#进入停车位
depot.notes_in_parking_space("2018-11-26 10:05:00")
#购物
person001.shopping()
#离开停车位
depot.notes_out_parking_space("2018-11-26 12:05:00")
#离开停车场
depot.notes_out_depot("2018-11-26 19:10:00")
depot.make_order()
#结束模拟流程
print()

