from parking_record import ParkingRecord
from depot import depot
from car import car

print("start")
car1 = car("辽A 888888")
parking_record1 = ParkingRecord()
depot1 = depot(parking_record1,car1)s

depot1.set_id(car1.get_id())
depot1.set_in_time("2018-11-22 11:11:11")
depot1.set_packing_time("2018-11-22 11:16:11")
car1.shopping()
depot1.set_out_packing_time("2018-11-22 13:16:11")
depot1.set_out_time("2018-11-22 13:11:11")
depot1.set_money()
depot1.end_parking()

print("end")

print(type(car1))