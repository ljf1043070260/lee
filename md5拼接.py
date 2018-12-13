from django.shortcuts import render

# Create your views here.
from django.views.generic import View
from identify_records.forms import IdentifyRecordsPostForm
from identify_records.models import IdentifyRecord
from parking_records.helper import ParkingRecordHelper
from utils.datetime_utils import now, dtt
from utils.decorator import json_required
from utils.forms import validate_form
from utils.helper import md5_hex_digest
from utils.responses import HttpJsonResponse
from cloud_client.django.caches.user import User
import logging

logger = logging.getLogger('default')


def check_user(request, parking_id=None):
    return User(request).user_info()


class IdentifyRecordsView(View):
    @json_required()
    def post(self, request, parking_id):
        flag, user = check_user(request)
        logger.info("check_user--flag:{}".format(flag))
        logger.info("check_user--user:{}".format(user))
        if not flag:
            return HttpJsonResponse(status=403)
        flag, data = validate_form(
            IdentifyRecordsPostForm, request.jsondata)
        if not flag:
            return HttpJsonResponse({
                "message": "Validation Failed",
                "errors": data
            }, status=422)
        floor_name = data['floor_name'] if data['floor_name'] else ''
        part_name = data['part_name'] if data['part_name'] else ''
        spot_num = data['spot_num'] if data['spot_num'] else ''
        md5_code = '%s-%s-%s' % (floor_name, part_name, spot_num)
        md5_value = md5_hex_digest(md5_code)
        identify_record = IdentifyRecord.objects.create(
            parking_id=parking_id,
            device_id=data['device_id'],
            collection_type=data['collection_type'],
            spot_num=data['spot_num'],
            floor_name=data['floor_name'],
            part_name=data['part_name'],
            spot_md5=md5_value,
            recognition_rate=data['recognition_rate'],
            user_id=user['htcode'],
            car_id=data['car_id'],
            identified_time=data['identified_time'] if data[
                'identified_time'] else dtt(now()),
            identified_pic=data['identified_pic'],
            identified_type=data['identified_type'],
            plate_color=data['plate_color'],
            plate_type=data['plate_type']
        )
        # 创建停车记录
        if identify_record.recognition_rate == 100:
            ParkingRecordHelper.create_or_update(identify_record)
        return HttpJsonResponse(
            {'identify_record_id': identify_record.identify_record_id,
             'created_time': identify_record.created_time},
            status=201)
