import random
from collections import OrderedDict
from datetime import timedelta
from io import BytesIO
import copy
import datetime
import logging
import time
import urllib
from urllib.parse import quote
import uuid
from django.db.models.query_utils import Q
from django.db.transaction import atomic
from django.http.response import HttpResponseForbidden, FileResponse, HttpResponse
from django.utils.decorators import method_decorator
from django.views.generic.base import View
from pyexcel_xlsx import save_data
import pyexcel

from bills.helper import BillDetailUtils
from company.models import Company
from download_center.models import DownloadRecord
from driver_type.helper import DriverTypeHelper
from driver_type.models import StopArea, DriverType
from oauth2.decorator import session_required
from parking.helper import ParkingHelper
from parking.models import Parking
from permissions.check import check_perm, check_in_perms
from product.forms import ParkingTimelyProductCreateForm, \
    ParkingCountProductCreateForm, CompanyCountProductCreateForm, \
    ParkingProductOrdersQueryForm, CompanyProductOrdersQueryForm, \
    ParkingUserServicesQueryForm, CompanyUserServicesQueryForm, \
    ParkingProductOrderCreateForm, CompanyProductOrderCreateForm, \
    ParkingUserServicePatchUserForm, ParkingUserServicePatchCarsForm, \
    CompanyUserServicePatchCarsForm, CompanyUserServicePatchUserForm, \
    CompanyCountProductPatchEnabledForm, ParkingTimelyProductUpdateForm, \
    ParkingCountProductUpdateForm, CompanyCountProductUpdateForm, \
    ParkingTimelyProductSoldCountForm, ParkingCountProductSoldCountForm, \
    CompanyCountProductSoldCountForm, ParkingTimelyProductsQueryForm, \
    ParkingCountProductsQueryForm, CompanyCountProductsQueryForm, \
    ParkingProductOrderRefundForm, ParkingUserServicesImportForm, \
    CompanyUserServicesImportForm, ParkingSuperProductOrderCreateForm, \
    ParkingTimelyProductPatchSpotsForm, ParkingTimelyProductPatchEnabledForm, \
    ParkingCountProductPatchEnabledForm, CompanyProductOrdersExportForm, \
    ParkingUserServicesQueryDueTimeForm, CompanyUserServicesQueryDueTimeForm,\
    ParkingProductOrderBatchCreateForm, CompanyProductOrderBatchCreateForm,\
    ParkingSuperProductOrderBatchCreateForm, ParkingProductOrderBatchRefundForm
from product.helper import get_operator, parking_spot, get_sales_count, \
    occupy_spots, product_message
from product.helper import query_services_within_a_period_of_time, \
    update_service_to_app
from product.models import ParkingTimelyProduct, ParkingCountProduct, \
    CompanyCountProduct, Order, UserService
from product.tasks import create_and_upload_parking_userservices_file, \
    create_and_upload_company_userservices_file, create_and_upload_parking_product_orders_file, \
    create_and_upload_company_product_orders_file
from product.utils import send_user_service_to_parking, \
    log_parking_timely_product, log_parking_timely_product_enabled, \
    log_parking_count_product, log_parking_count_product_enabled, \
    log_parking_product_order, log_parking_product_refund, \
    log_parking_userservices_car, log_company_count_product, \
    log_company_count_product_enabled, log_company_product_order, \
    log_company_product_refund, log_company_userservices_car, \
    log_parking_userservice_import, log_company_userservice_import, \
    log_parking_userservice_export, log_company_userservice_export
from short_message.models import ShortMessageUseRecord, CompanyShortMessageNumber, ShortMessageOrder, \
    ShortMessageService, CompanyShortMessageSetting, ParkingShortMessageSetting
from utils.datetime_utils import now, addmonths, timestamp_to_datetime as ttd, \
    datetime_to_timestamp as dtt, get_micronseconds
from utils.forms import validate_form
from utils.helper import get_local_host, timestamp_to_datetime, RequestParamsHandle, matching_telephone_simpleness, is_vaild_date, matching_many_car_id
from utils.responses import HttpJsonResponse, errors_422
import json

logger = logging.getLogger('default')


class ParkingTimelyProductView(View):
    @session_required()
    def get(self, request, parking_id, timely_product_id):
        if not check_perm(
                request, 'parking_timely_products:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        try:
            product = ParkingTimelyProduct.objects.get(
                parking_id=parking_id, timely_product_id=timely_product_id)
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(status=404)

        if product.deleted:
            return HttpJsonResponse(status=410)
        return HttpJsonResponse(product.detail_info())

    @session_required()
    def put(self, request, parking_id, timely_product_id):
        if not check_perm(
                request, 'parking_timely_products:update',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingTimelyProductUpdateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            product = ParkingTimelyProduct.objects.get(
                parking_id=parking_id, timely_product_id=timely_product_id)
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(status=404)

        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'parking', 'code': 'missing_field'}
                ]
            }, status=422)

        if product.deleted:
            return HttpJsonResponse(status=410)

        if ParkingTimelyProduct.objects.filter(
                timely_product_name=data['timely_product_name'],
                parking_id=parking_id,
                deleted=False
        ).exclude(timely_product_id=timely_product_id):
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'timely_product_name',
                     'code': 'common'}
                ]
            }, status=422)

        ParkingTimelyProduct.objects.filter(
            timely_product_id=timely_product_id).update(**data)
        code = 'timely:%s' % timely_product_id
        DriverTypeHelper.update_driver_subtype(parking_id,
                                               code,
                                               data['timely_product_name'])
        # 记日志
        log_parking_timely_product(request, parking, product, data, 'update')
        return HttpJsonResponse(status=204)

    @session_required()
    def delete(self, request, parking_id, timely_product_id):
        if not check_perm(
                request, 'parking_timely_products:delete',
                parking_id=parking_id):
            return HttpResponseForbidden()
        try:
            product = ParkingTimelyProduct.objects.get(
                parking_id=parking_id, timely_product_id=timely_product_id)
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        current_time = now()
        query = 'select * from product_userservice where use_scope @> %s'
        params = ['{%s}' % parking_id]
        query += ' and product_id = \'%s\'' % product.timely_product_id.hex
        query += " and service_status = 1"
        query += " and surplus_count > 0"
        query += " and end_time > %s"
        params.append(current_time)
        services = UserService.objects.raw(query, params)
        if len(list(services)) > 0:
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'resource': 'product',
                     'code': 'has_valid_services'}
                ]
            }, status=422)
        product.deleted = True
        product.deleted_time = now()
        product.save()
        code = 'timely:%s' % timely_product_id
        DriverTypeHelper.delete_driver_subtype(parking_id, code)
        # 记日志
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        log_parking_timely_product(request, parking, product, None, 'delete')
        return HttpJsonResponse(status=204)


class ParkingTimelyProductSoldCountView(View):
    @session_required()
    def get(self, request, parking_id, timely_product_id):
        if not check_perm(
                request, 'parking_timely_products:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingTimelyProductSoldCountForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        sold_count = query_services_within_a_period_of_time(
            data['begin_time'],
            data['end_time'],
            product_id=timely_product_id
        ).count()
        code = 'timely:%s' % timely_product_id
        occupy_count = occupy_spots(
            code, data['begin_time'], data['end_time'], timely_product_id)
        return HttpJsonResponse({
            'timely_product_id': timely_product_id,
            'sold_count': sold_count + len(occupy_count)
        })


class ParkingTimelyProductSpotsView(View):
    @session_required()
    def patch(self, request, parking_id, timely_product_id):
        if not check_perm(
                request, 'parking_timely_products:update',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingTimelyProductPatchSpotsForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            product = ParkingTimelyProduct.objects.get(
                parking_id=parking_id, timely_product_id=timely_product_id)
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        if product.deleted:
            return HttpJsonResponse(status=410)

        product.spots = data['spots']
        product.save()

        # 记日志
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        log_parking_timely_product_enabled(request, parking, product)
        return HttpJsonResponse(status=204)


class ParkingTimelyProductEnabledView(View):
    @session_required()
    def patch(self, request, parking_id, timely_product_id):
        if not check_perm(
                request, 'parking_timely_products:update',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingTimelyProductPatchEnabledForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            product = ParkingTimelyProduct.objects.get(
                parking_id=parking_id, timely_product_id=timely_product_id)
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        if product.deleted:
            return HttpJsonResponse(status=410)

        product.enabled = data['enabled']
        product.save()

        # 记日志
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        log_parking_timely_product_enabled(request, parking, product)
        return HttpJsonResponse(status=204)


class ParkingTimelyProductsView(View):
    @session_required()
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_timely_products:add',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingTimelyProductCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'parking', 'code': 'missing_field'}
                ]
            }, status=422)

        if ParkingTimelyProduct.objects.filter(
                timely_product_name=data['timely_product_name'],
                parking_id=parking_id,
                deleted=False
        ):
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [{
                    'field': 'timely_product_name',
                    'code': 'common'
                }]}, status=422)

        data['parking_id'] = parking_id
        product = ParkingTimelyProduct.objects.create(**data)

        # 记日志
        log_parking_timely_product(request, parking, product, None, 'add')
        driver_type = DriverType.objects.filter(parking_id=parking_id,
                                                code='timely').first()
        code = 'timely:%s' % product.timely_product_id.hex
        name = product.timely_product_name
        category = 'original'
        driver_subtype = DriverTypeHelper.create_driver_subtype(name,
                                                                parking,
                                                                code,
                                                                category,
                                                                driver_type)
        return HttpJsonResponse({
            'timely_product_id': product.timely_product_id.hex,
            'driver_type_id': driver_subtype.driver_type_id,
            'drvier_subtype_id': driver_subtype.driver_subtype_id,
            'created_time': dtt(product.created_time)
        }, status=201)

    @session_required()
    def get(self, request, parking_id):
        if not check_in_perms(
                request, [
                    {'parking_id': parking_id,
                     'code': 'parking_timely_products:view'},
                    {'parking_id': parking_id, 'code': 'statistics:view'}
                ]):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingTimelyProductsQueryForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        q = Q(deleted=False)
        q &= Q(purchase_mode__contains=[2])
        q &= Q(parking_id=parking_id)
        if data['section'][0]:
            q &= Q(created_time__gt=data['section'][0])
        if data['section'][1]:
            q &= Q(created_time__lt=data['section'][1])

        products = ParkingTimelyProduct.objects.filter(
            q).order_by(*data['order_by'])[:data['limit'] + 1]

        has_next = False
        if len(products) == data['limit'] + 1:
            has_next = True
        stop_stamp = data['section'][1] if data['section'][1] else now()
        products = products[:data['limit']]
        results = []
        for product in products:
            results.append(product.detail_info())
            stop_stamp = product.created_time
        stop_stamp = dtt(stop_stamp)
        start_stamp = dtt(data['section'][0]) if data['section'][0] else 0
        resp = HttpJsonResponse(results)
        if has_next:
            params = 'section=%.6f,%.6f' % (
                start_stamp, stop_stamp)
            if data['limit']:
                params = params + '&limit=%d' % data['limit']
            if data['order_by']:
                params = params + '&order_by=%s' % ','.join(data['order_by'])
            resp['Link'] = r'<%s%s?%s>; rel="next"' % (
                get_local_host(request), request.path, params)
        return resp

    def _nextpage_link(self, request, products):
        return ''


class ParkingCountProductView(View):
    @session_required()
    def get(self, request, parking_id, count_product_id):
        if not check_perm(
                request, 'parking_count_products:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        try:
            product = ParkingCountProduct.objects.get(
                parking_id=parking_id, count_product_id=count_product_id)
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)

        if product.deleted:
            return HttpJsonResponse(status=410)
        return HttpJsonResponse(product.detail_info())

    @session_required()
    def put(self, request, parking_id, count_product_id):
        if not check_perm(
                request, 'parking_count_products:update',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingCountProductUpdateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            product = ParkingCountProduct.objects.get(
                parking_id=parking_id, count_product_id=count_product_id)
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)

        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'parking', 'code': 'missing_field'}
                ]
            }, status=422)

        if product.deleted:
            return HttpJsonResponse(status=410)

        if ParkingCountProduct.objects.filter(
                count_product_name=data['count_product_name'],
                parking_id=parking_id,
                deleted=False
        ).exclude(count_product_id=count_product_id):
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'count_product_name',
                     'code': 'common'}
                ]
            }, status=422)

        ParkingCountProduct.objects.filter(
            count_product_id=count_product_id).update(**data)
        code = 'count:%s' % count_product_id
        DriverTypeHelper.update_driver_subtype(parking_id,
                                               code,
                                               data['count_product_name'])
        # 记日志
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        log_parking_count_product(request, parking, product, data, 'update')
        return HttpJsonResponse(status=204)

    @session_required()
    def delete(self, request, parking_id, count_product_id):
        if not check_perm(
                request, 'parking_count_products:delete',
                parking_id=parking_id):
            return HttpResponseForbidden()
        try:
            product = ParkingCountProduct.objects.get(
                parking_id=parking_id, count_product_id=count_product_id)
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        current_time = now()
        query = 'select * from product_userservice where use_scope @> %s'
        params = ['{%s}' % parking_id]
        query += ' and product_id = \'%s\'' % product.count_product_id.hex
        query += " and service_status = 1"
        query += " and surplus_count > 0"
        query += " and end_time > %s"
        params.append(current_time)
        services = UserService.objects.raw(query, params)
        if len(list(services)) > 0:
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'resource': 'product',
                     'code': 'has_valid_services'}
                ]
            }, status=422)
        product.deleted = True
        product.deleted_time = now()
        product.save()
        code = 'count:%s' % count_product_id
        DriverTypeHelper.delete_driver_subtype(parking_id, code)
        # 记日志
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        log_parking_count_product(request, parking, product, None, 'delete')
        return HttpJsonResponse(status=204)


class ParkingCountProductSoldCountView(View):
    @session_required()
    def get(self, request, parking_id, count_product_id):
        if not check_perm(
                request, 'parking_count_products:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingCountProductSoldCountForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        sold_count = query_services_within_a_period_of_time(
            data['begin_time'],
            data['end_time'],
            product_id=count_product_id
        ).count()
        code = 'count:%s' % count_product_id
        occupy_count = occupy_spots(
            code, data['begin_time'], data['end_time'], count_product_id)
        return HttpJsonResponse({
            'count_product_id': count_product_id,
            'sold_count': sold_count + len(occupy_count)
        })


class ParkingCountProductEnabledView(View):
    @session_required()
    def patch(self, request, parking_id, count_product_id):
        if not check_perm(
                request, 'parking_count_products:update',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingCountProductPatchEnabledForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            product = ParkingCountProduct.objects.get(
                parking_id=parking_id, count_product_id=count_product_id)
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        if product.deleted:
            return HttpJsonResponse(status=410)

        product.enabled = data['enabled']
        product.save()

        # 记日志
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        log_parking_count_product_enabled(request, parking, product)
        return HttpJsonResponse(status=204)


class ParkingCountProductsView(View):
    @session_required()
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_count_products:add',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingCountProductCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)

        if ParkingCountProduct.objects.filter(
                count_product_name=data['count_product_name'],
                parking_id=parking_id,
                deleted=False
        ):
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [{
                    'field': 'count_product_name',
                    'code': 'common'
                }]}, status=422)

        data['parking_id'] = parking_id
        product = ParkingCountProduct.objects.create(**data)
        driver_type = DriverType.objects.filter(parking_id=parking_id,
                                                code='count').first()
        code = 'count:%s' % product.count_product_id.hex
        name = product.count_product_name
        category = 'original'
        driver_subtype = DriverTypeHelper.create_driver_subtype(name,
                                                                parking,
                                                                code,
                                                                category,
                                                                driver_type)
        # 记日志
        log_parking_count_product(request, parking, product, None, 'add')
        return HttpJsonResponse({
            'count_product_id': product.count_product_id.hex,
            'driver_type_id': driver_subtype.driver_type_id,
            'drvier_subtype_id': driver_subtype.driver_subtype_id,
            'created_time': dtt(product.created_time)
        }, status=201)

    @session_required()
    def get(self, request, parking_id):
        if not check_in_perms(
                request, [
                    {'parking_id': parking_id,
                     'code': 'parking_count_products:view'},
                    {'parking_id': parking_id, 'code': 'statistics:view'}
                ]):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingCountProductsQueryForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        q = Q(deleted=False)
        q &= Q(parking_id=parking_id)
        q &= Q(purchase_mode__contains=[2])
        if data['section'][0]:
            q &= Q(created_time__gt=data['section'][0])
        if data['section'][1]:
            q &= Q(created_time__lt=data['section'][1])

        products = ParkingCountProduct.objects.filter(
            q).order_by(*data['order_by'])[:data['limit'] + 1]

        has_next = False
        if len(products) == data['limit'] + 1:
            has_next = True

        stop_stamp = data['section'][1] if data['section'][1] else now()
        products = products[:data['limit']]
        results = []
        for product in products:
            results.append(product.detail_info())
            stop_stamp = product.created_time
        stop_stamp = dtt(stop_stamp)
        start_stamp = dtt(data['section'][0]) if data['section'][0] else 0
        resp = HttpJsonResponse(results)
        if has_next:
            params = 'section=%.6f,%.6f' % (
                start_stamp, stop_stamp)
            if data['limit']:
                params = params + '&limit=%d' % data['limit']
            if data['order_by']:
                params = params + '&order_by=%s' % ','.join(data['order_by'])
            resp['Link'] = r'<%s%s?%s>; rel="next"' % (
                get_local_host(request), request.path, params)
        return resp

    def _nextpage_link(self, request, products):
        return ''


class CompanyCountProductView(View):
    @session_required()
    def get(self, request, company_id, count_product_id):
        if not check_perm(
                request, 'company_count_products:view',
                company_id=company_id):
            return HttpResponseForbidden()
        try:
            product = CompanyCountProduct.objects.get(
                company_id=company_id, count_product_id=count_product_id)
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)

        if product.deleted:
            return HttpJsonResponse(status=410)
        return HttpJsonResponse(product.detail_info())

    @session_required()
    def put(self, request, company_id, count_product_id):
        if not check_perm(
                request, 'company_count_products:update',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyCountProductUpdateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            product = CompanyCountProduct.objects.get(
                company_id=company_id, count_product_id=count_product_id)
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)

        if product.deleted:
            return HttpJsonResponse(status=410)

        if CompanyCountProduct.objects.filter(
                count_product_name=data['count_product_name'],
                company_id=company_id,
                deleted=False
        ).exclude(count_product_id=count_product_id):
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'count_product_name',
                     'code': 'common'}
                ]
            }, status=422)

        CompanyCountProduct.objects.filter(
            count_product_id=count_product_id).update(**data)
        code = 'company_count:%s' % count_product_id
        for parking_id in data['use_scope']:
            try:
                parking = Parking.objects.get(parking_id=parking_id)
            except Parking.DoesNotExist:
                continue
            driver_type = DriverType.objects.filter(
                company_id=company_id,
                parking_id=parking_id,
                code='company_count').first()
            if not driver_type:
                driver_type, _ = DriverType.objects.get_or_create(
                    company_id=company_id,
                    parking_id=parking_id,
                    code='company_count',
                    defaults={
                        'driver_type_name': '企业包次用户',
                        'category': 'original',
                    }
                )
            category = 'original'
            DriverTypeHelper.update_or_create_driver_subtype(data['count_product_name'],
                                                             parking,
                                                             code,
                                                             category,
                                                             driver_type)
        # 记日志
        log_company_count_product(request, company_id, product, data, 'update')
        return HttpJsonResponse(status=204)

    @session_required()
    def delete(self, request, company_id, count_product_id):
        if not check_perm(
                request, 'company_count_products:delete',
                company_id=company_id):
            return HttpResponseForbidden()
        try:
            product = CompanyCountProduct.objects.get(
                company_id=company_id, count_product_id=count_product_id)
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        current_time = now()
        params = []
        query = 'select * from product_userservice where'
        query += ' product_id = \'%s\'' % product.count_product_id.hex
        query += " and service_status = 1"
        query += " and surplus_count > 0"
        query += " and end_time > %s"
        params.append(current_time)
        services = UserService.objects.raw(query, params)
        if len(list(services)) > 0:
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'resource': 'product',
                     'code': 'has_valid_services'}
                ]
            }, status=422)
        product.deleted = True
        product.deleted_time = now()
        product.save()
        try:
            parkings = Parking.objects.filter(company_id=company_id)
        except:
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'parking', 'code': 'missing_field'}
                ]
            }, status=422)
        code = 'company_count:%s' % count_product_id
        for parking in parkings:
            DriverTypeHelper.delete_driver_subtype(parking.parking_id, code)
        # 记日志
        log_company_count_product(request, company_id, product, None, 'delete')
        return HttpJsonResponse(status=204)


class CompanyCountProductSoldCountView(View):
    @session_required()
    def get(self, request, company_id, count_product_id):
        is_valid, data = validate_form(
            CompanyCountProductSoldCountForm, request.GET)
        if data['parking_id']:
            if not check_perm(
                    request, 'parking_count_products:view',
                    parking_id=data['parking_id']) and not check_perm(
                request, 'company_count_products:view',
                company_id=company_id):
                return HttpResponseForbidden()
        else:
            if not check_perm(
                    request, 'company_count_products:view',
                    company_id=company_id):
                return HttpResponseForbidden()
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        sold_count = query_services_within_a_period_of_time(
            data['begin_time'],
            data['end_time'],
            product_id=count_product_id
        ).count()
        code = 'company_count:%s' % count_product_id
        occupy_count = occupy_spots(
            code, data['begin_time'], data['end_time'], count_product_id)
        return HttpJsonResponse({
            'count_product_id': count_product_id,
            'sold_count': sold_count + len(occupy_count)
        })


class CompanyCountProductEnabledView(View):
    @session_required()
    def patch(self, request, company_id, count_product_id):
        if not check_perm(
                request, 'company_count_products:update',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyCountProductPatchEnabledForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            product = CompanyCountProduct.objects.get(
                count_product_id=count_product_id)
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        if product.deleted:
            return HttpJsonResponse(status=410)

        product.enabled = data['enabled']
        product.save()
        # 记日志
        log_company_count_product_enabled(request, company_id, product)
        return HttpJsonResponse(status=204)


class CompanyCountProductsView(View):
    @session_required()
    def post(self, request, company_id):
        if not check_perm(
                request, 'company_count_products:add',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyCountProductCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        if CompanyCountProduct.objects.filter(
                count_product_name=data['count_product_name'],
                company_id=company_id,
                deleted=False
        ):
            return HttpJsonResponse({
                'message': 'Validation Failed',
                'errors': [
                    {'field': 'count_product_name',
                     'code': 'common'}
                ]
            }, status=422)
        data['company_id'] = company_id
        product = CompanyCountProduct.objects.create(**data)
        parking_ids = data['use_scope']
        for parking_id in parking_ids:
            try:
                parking = Parking.objects.get(parking_id=parking_id)
            except Parking.DoesNotExist:
                continue
            driver_type = DriverType.objects.filter(
                company_id=company_id,
                parking_id=parking_id,
                code='company_count').first()
            if not driver_type:
                driver_type, _ = DriverType.objects.get_or_create(
                    company_id=company_id,
                    parking_id=parking_id,
                    code='company_count',
                    defaults={
                        'driver_type_name': '企业包次用户',
                        'category': 'original',
                    }
                )
            code = 'company_count:%s' % product.count_product_id.hex
            name = product.count_product_name
            category = 'original'
            DriverTypeHelper.create_driver_subtype(name,
                                                   parking,
                                                   code,
                                                   category,
                                                   driver_type)
        # 记日志
        log_company_count_product(request, company_id, product, None, 'add')
        return HttpJsonResponse({
            'timely_product_id': product.count_product_id.hex,
            'created_time': dtt(product.created_time)
        }, status=201)

    @session_required()
    def get(self, request, company_id):
        is_valid, data = validate_form(
            CompanyCountProductsQueryForm, request.GET)
        if data['parking_id']:
            if not check_perm(
                    request, 'parking_count_products:view',
                    parking_id=data['parking_id']) and not check_perm(
                request, 'company_count_products:view',
                company_id=company_id):
                return HttpResponseForbidden()
        else:
            pass
        # if not check_perm(
        #                     request, 'company_count_products:view',
        #                     company_id=company_id):
        #                 return HttpResponseForbidden()
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        q = Q(deleted=False)
        q &= Q(purchase_mode__contains=[2])
        q &= Q(company_id=company_id)
        if data['parking_id']:
            q &= Q(use_scope__contains=[data['parking_id']])
        if data['section'][0]:
            q &= Q(created_time__gt=data['section'][0])
        if data['section'][1]:
            q &= Q(created_time__lt=data['section'][1])

        products = CompanyCountProduct.objects.filter(
            q).order_by(*data['order_by'])[:data['limit'] + 1]

        has_next = False
        if len(products) == data['limit'] + 1:
            has_next = True

        stop_stamp = data['section'][1] if data['section'][1] else now()
        products = products[:data['limit']]
        results = []
        for product in products:
            results.append(product.detail_info())
            stop_stamp = product.created_time
        stop_stamp = dtt(stop_stamp)
        start_stamp = dtt(data['section'][0]) if data['section'][0] else 0
        resp = HttpJsonResponse(results)
        if has_next:
            params = 'section=%.6f,%.6f' % (
                start_stamp, stop_stamp)
            if data['limit']:
                params = params + '&limit=%d' % data['limit']
            if data['parking_id']:
                params = params + '&parking_id=%s' % data['parking_id']
            if data['order_by']:
                params = params + '&order_by=%s' % ','.join(data['order_by'])
            resp['Link'] = r'<%s%s?%s>; rel="next"' % (
                get_local_host(request), request.path, params)
        return resp

    def _nextpage_link(self, request, products):
        return ''


class ParkingProductOrdersView(View):
    @session_required()
    def get(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_orders:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingProductOrdersQueryForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        # q = Q(use_scope__contains=[parking_id])
        q = Q(where_bought=parking_id)
        if data['section'][0]:
            q &= Q(created_time__gte=data['section'][0])
        if data['section'][1]:
            q &= Q(created_time__lt=data['section'][1])
        if data['paid_section'][0]:
            q &= Q(paid_time__gte=data['paid_section'][0])
        if data['paid_section'][1]:
            q &= Q(paid_time__lt=data['paid_section'][1])
        if data['product_type']:
            q &= Q(product_type=data['product_type'])
        if data['product_name']:
            q &= Q(product_name__contains=data['product_name'])
        if data['telephone']:
            q &= Q(telephone__contains=data['telephone'])
        if data['car_id']:
            q &= Q(car_ids__contains=[data['car_id']])
        if data['order_id']:
            q &= Q(order_id=data['order_id'])
        if data['order_status']:
            q &= Q(order_status=data['order_status'])
        if data['paid_way']:
            # q &= Q(paid_way=data['paid_way'])
            if data['paid_way'] == 'SYSTEM':
                q1 = Q(paid_way__isnull=True)
                q1 |= Q(paid_way='')
                q &= Q(q1)
            elif data['paid_way'] in ['WX', 'ALI', 'uu', 'UU']:
                q1 = Q(paid_way__istartswith=data['paid_way'].upper())
                q1 |= Q(paid_way__istartswith=data['paid_way'].lower())
                q &= Q(q1)
            else:
                q &= Q(paid_way__iexact=data['paid_way'])
        queryset = Order.objects.filter(q).order_by(
            *data['order_by'])[data['page_size'] * (
                data['page_num'] - 1):(data['page_num'] * data['page_size'] + 1)]
        result = [_.detail_info() for _ in queryset[:data['page_size']]]
        resp = HttpJsonResponse(result)
        if len(queryset) > data['page_size']:
            params = 'page_num=%d&page_size=%d' % (
                data['page_num'] + 1, data['page_size'])
            if data['order_by']:
                params = params + '&order_by=%s' % ','.join(data['order_by'])
            if data['paid_section'][0] is not None and data['paid_section'][1] is not None:
                params += '&paid_section=%.6f,%.6f' % (
                    dtt(data['paid_section'][0]), dtt(data['paid_section'][1]))
            if data['section'][0] is not None and data['section'][1] is not None:
                params += '&section=%.6f,%.6f' % (
                    dtt(data['section'][0]), dtt(data['section'][1]))
            if data['product_name']:
                params = params + '&product_name=%s' % data['product_name']
            if data['product_type']:
                params = params + '&product_type=%s' % data['product_type']
            if data['product_name']:
                params = params + '&product_name=%s' % data['product_name']
            if data['telephone']:
                params = params + '&telephone=%s' % data['telephone']
            if data['car_id']:
                params = params + '&car_id=%s' % data['car_id']
            if data['paid_way']:
                params = params + '&paid_way=%s' % data['paid_way']
            if data['order_status']:
                params = params + '&order_status=%d' % data['order_status']
            resp['Link'] = r'<%s%s?%s>; rel="next"' % (
                get_local_host(request), request.path, params)
        return resp

    def _order_by_paid_time(self, data, orders):
        results = []
        stop_time = None
        params = None
        for order in orders[:data['limit']]:
            results.append(order.detail_info())
            stop_time = order.paid_time
        if len(orders) > data['limit']:
            params = 'limit=%d' % data['limit']
            params += '&order_by=%s' % ','.join(data['order_by'])
            if data['order_by'][0].startswith('-'):
                params += '&paid_section=%.6f,%.6f' % (
                    dtt(data['paid_section'][0]), dtt(stop_time))
            else:
                params += '&paid_section=%.6f,' % dtt(stop_time)
                if data['paid_section'][1]:
                    params += '%.6f' % dtt(data['paid_section'][1])
            if data['section'][0]:
                params += '&section=%.6f,' % dtt(
                    data['section'][0])
                if data['section'][1]:
                    params += '%.6f' % dtt(data['section'][1])
        return results, params

    def _order_by_created_time(self, data, orders):
        results = []
        stop_time = None
        params = None
        for order in orders[:data['limit']]:
            results.append(order.detail_info())
            stop_time = order.created_time
        if len(orders) > data['limit']:
            params = 'limit=%d' % data['limit']
            params += '&order_by=%s' % ','.join(data['order_by'])
            if data['order_by'][0].startswith('-'):
                params += '&section=%.6f,%.6f' % (
                    dtt(data['section'][0]), dtt(stop_time))
            else:
                params += '&section=%.6f,' % dtt(stop_time)
                if data['section'][1]:
                    params += '%.6f' % dtt(data['section'][1])
            if data['paid_section'][0]:
                params += '&paid_section=%.6f,' % dtt(
                    data['paid_section'][0])
                if data['paid_section'][1]:
                    params += '%.6f' % dtt(data['paid_section'][1])
        return results, params

    def _nextpage_link(self, request, orders):
        return ''

    @session_required()
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_orders:add',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingProductOrderCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        data['company_id'] = ParkingHelper.get(parking_id).company_id
        data['where_bought'] = parking_id
        if data['product_type'] == 1:
            datas = self._buy_timely_product(request, data)
            return datas
        elif data['product_type'] == 2:
            datas = self._buy_count_product(request, data)
            return datas
        else:
            return HttpJsonResponse(errors_422([
                {'field': 'product_type', 'code': 'invalid'}
            ]), status=422)

    def _build_order(self, params, product):
        #         data = {"mobile": params['telephone']}
        #         user_id = get_htcode(data)
        order = {
            #             'user_id': user_id,
            'product_id': params['product_id'],
            'product_type': params['product_type'],
            'begin_time': params['start_date'],
            'car_ids': params['car_ids'],
            'use_scope': [product.parking_id],
            'parallel_num': product.parallel_num,
            # 'price': product.price,
            # 'paid_amount': params['paid_amount'],
            'paid_way': 'CASH',
            'paid_time': now(),
            'order_status': 2,
            'telephone': params['telephone'],
            'company_id': params['company_id'],
            'where_bought': params['where_bought'],
        }
        if product.duration_unit == 1:
            duration = timedelta(days=product.duration)
            order['end_time'] = params['start_date'] + duration
        else:
            end_time = addmonths(params['start_date'], product.duration)
            if end_time.day < params['start_date'].day:
                end_time += timedelta(days=1)
                end_time = end_time.replace(hour=0, minute=0, second=0)
            order['end_time'] = end_time
        if params['product_type'] == 1 and product.start_cycle_unit in [2, 3]:
            price, static_time = product.static_time_with_part_prices(
                params['start_date'], order['end_time'])
            order['paid_amount'] = params['paid_amount']
            order['price'] = price
            order['end_time'] = static_time
        else:
            order['paid_amount'] = params['paid_amount']
            order['price'] = product.price
        if params['product_type'] == 1:
            order['product_name'] = product.timely_product_name
        else:
            order['product_name'] = product.count_product_name
        return order

    def _build_service(self, product, order, params):
        #         data = {"mobile": order['telephone']}
        #         user_id = get_htcode(data)
        service = {
            #             'user_id': user_id,
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'service_detail': product.detail_info(),
            'car_type': product.car_type,
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'telephone': order['telephone'],
            'company_id': order['company_id'],
            'where_bought': order['where_bought'],
            'username': params['username'],
            'address': params['address'],
            'comments': params['comments']

        }
        if order['product_type'] == 1:
            service['surplus_count'] = 9999
        else:
            service['surplus_count'] = product.count

        return service

    def _query_already_services_count(self, order):
        return query_services_within_a_period_of_time(
            order['begin_time'], order['end_time'],
            product_id=order['product_id']).count()

    @method_decorator(atomic)
    def _buy_timely_product(self, request, params):
        try:
            product = ParkingTimelyProduct.objects.get(
                timely_product_id=params['product_id'])
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_timely_product', 'code': 'not_found'}
            ]), status=422)
        spot = parking_spot('timely:%s' % product.timely_product_id.hex)
        if product.deleted:
            return HttpJsonResponse(status=410)
        if not product.enabled:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_timely_product', 'code': 'not_allow'}
            ]), status=422)
        if 2 not in product.purchase_mode:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_timely_product', 'code': 'not_allow'}
            ]), status=422)
        order = self._build_order(params, product)
        service = self._build_service(product, order, params)

        service_count = self._query_already_services_count(order)
        if service_count >= product.sales_count:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_timely_product', 'code': 'number_limits'}
            ]), status=422)

        order = Order.objects.create(**order)
        service['order_id'] = order.order_id.hex
        service['spots'] = spot
        service['used_quota_detail'] = {
            'quota_type': product.quota_type,
            'quota': product.quota,
            'quota_of_once': product.quota_of_once,
            'used_quota': {}
        }
        service = UserService.objects.create(**service)
        product_message(order)
        send_user_service_to_parking(service, 'create')
        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.use_scope[0] if order.product_type != 3 else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'service', 'income', '产品购买',
            parking.company_id, order.paid_way, order.paid_time,
            parking_id, operator,
            order.detail_info(), order.paid_amount)
        # 记日志
        log_parking_product_order(
            request,
            parking,
            order,
            'buy',
            'timely',
            params['username']
        )
        return HttpJsonResponse({
            'order_id': order.order_id.hex,
            'created_time': dtt(order.created_time),
            'service_id': service.service_id.hex,
            'spots': spot
        }, status=201)

    @method_decorator(atomic)
    def _timely_product_short_message(self, order, product):
        company_setting = CompanyShortMessageSetting.objects.filter(
            company_id=order.company_id).first()
        if not company_setting:
            return False
        if not company_setting.is_open_sms_alert:
            return False

        company_num = CompanyShortMessageNumber.objects.filter(
            company_id=order.company_id).first()
        if not company_num:
            return False
        if company_num.surplus_num <= 0:
            return False
        # 停车场包时
        if order.product_type in [1, 2, 4]:
            parking_setting = ParkingShortMessageSetting.objects.filter(
                company_id=order.company_id,
                parking_id=product.parking_id,
                key='service'
            ).first()
            if not parking_setting:
                return False
            logger.exception('停车场包时产品购买停车场开关:%s' % bool(parking_setting.value))
            if parking_setting.value == str(False):
                return False
        logger.exception('停车场包时产品购买停车场开关:%s 继续往下走' % bool(parking_setting.value))
        # 短信服务更新
        message_service = None
        message_orders = ShortMessageOrder.objects.filter(
            company_id=order.company_id,
            order_status=2
        ).order_by('created_time')
        for orde in message_orders:
            message_service = ShortMessageService.objects.filter(
                short_message_order=orde,
                surplus_num__gt=0
            ).first()
            if message_service:
                break
        if not message_service:
            return False
        # 创建使用记录
        use_record = {
            'surplus_num': company_num.surplus_num - 1,
            'use_num': 1,
            'operation_type': 'service',
            'company_id': order.company_id,
            'use_time': now(),
            'parking_id': product.parking_id,
            'telephone': order.telephone,
            'car_id': ','.join(order.car_ids),
            'short_message_service': message_service
        }
        ShortMessageUseRecord.objects.create(**use_record)
        company_num.surplus_num -= 1
        company_num.save()
        message_service.surplus_num -= 1
        message_service.save()
        # 短信提醒
        product_message(order)

    @method_decorator(atomic)
    def _buy_count_product(self, request, params):
        try:
            product = ParkingCountProduct.objects.get(
                count_product_id=params['product_id'])
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_count_product', 'code': 'not_found'}
            ]), status=422)
        spot = parking_spot('count:%s' % product.count_product_id.hex)
        if product.deleted:
            return HttpJsonResponse(status=410)
        if not product.enabled:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_count_product', 'code': 'not_allow'}
            ]), status=422)
        if 2 not in product.purchase_mode:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_count_product', 'code': 'not_allow'}
            ]), status=422)
        order = self._build_order(params, product)
        service = self._build_service(product, order, params)

        service_count = self._query_already_services_count(order)
        if service_count >= product.sales_count:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_count_product', 'code': 'number_limits'}
            ]), status=422)

        order = Order.objects.create(**order)
        service['order_id'] = order.order_id.hex
        service['spots'] = spot
        service = UserService.objects.create(**service)
        product_message(order)
        send_user_service_to_parking(service, 'create')
        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.use_scope[0] if order.product_type != 3 else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'service', 'income', '产品购买',
            parking.company_id, order.paid_way, order.paid_time,
            parking_id, operator,
            order.detail_info(), order.paid_amount)
        # 记日志
        log_parking_product_order(request,
                                  parking,
                                  order,
                                  'buy',
                                  'count',
                                  params['username'])
        return HttpJsonResponse({
            'order_id': order.order_id.hex,
            'created_time': dtt(order.created_time),
            'service_id': service.service_id.hex,
            'spots': spot
        }, status=201)

    @method_decorator(atomic)
    def _count_product_short_message(self, order, product):
        if order and order.company_id:
            company_setting = CompanyShortMessageSetting.objects.filter(
                company_id=order.company_id).first()
            if not company_setting:
                return
            if not company_setting.is_open_sms_alert:
                return
        # 停车场包次
        if order and order.product_type in [1, 2, 4]:
            parking_setting = ParkingShortMessageSetting.objects.filter(
                company_id=order.company_id,
                parking_id=product.parking_id,
                key='service'
            ).first()
            if not parking_setting:
                return
            if parking_setting.value == str(False):
                return
        # 短信服务更新
        message_service = ''
        message_orders = ShortMessageOrder.objects.filter(
            company_id=order.company_id,
            order_status=2
        ).order_by('created_time')
        for orde in message_orders:
            message_service = ShortMessageService.objects.filter(
                short_message_order=orde,
                surplus_num__gt=0
            ).first()
            if message_service:
                message_service.surplus_num -= 1
                message_service.save()
                break

        # 创建使用记录
        company_num = CompanyShortMessageNumber.objects.filter(
            company_id=order.company_id).first()
        if company_num and company_num.surplus_num > 0:
            use_record = {
                'surplus_num': company_num.surplus_num - 1,
                'use_num': 1,
                'operation_type': 'service',
                'company_id': order.company_id,
                'use_time': now(),
                'parking_id': product.parking_id,
                'telephone': order.telephone,
                'car_id': ','.join(order.car_ids),
                'short_message_service': message_service
            }
            ShortMessageUseRecord.objects.create(**use_record)
            company_num.surplus_num -= 1
            company_num.save()
        # 短信提醒
        product_message(order)


class ParkingProductOrdersExportView(View):
    @session_required()
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_orders:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyProductOrdersExportForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        if data['paid_section']:
            pay_min_stamp, pay_max_stamp = \
                RequestParamsHandle.get_time_range(data['paid_section'])
            if pay_min_stamp is None and pay_max_stamp is None:
                return HttpResponse(json.dumps({
                    'message': 'Validation Failed', 'errors': [{
                        'field': 'section', 'code': 'invalid'}]
                }, ensure_ascii=False), status=422)
        result = {}
        time_now = time.strftime('%Y%m%d-%H%M%S', time.localtime())
        result['category'] = 'product_orders'
        file_name = '%s-%s.xlsx' % ('product_orders_list', time_now)
        result['parking_id'] = parking_id
        result['create_user'] = request.user.htcode
        result['file_name'] = file_name
        result['file_size'] = ''
        result['status'] = 2
        result['expire_time'] = timestamp_to_datetime(
            time.time() + 7 * 24 * 3600)
        download_record = DownloadRecord.objects.create(**result)
        request_data = {'is_superuser': request.user.is_superuser,
                        'username': request.user.htcode,
                        'remote_addr': request.remote_addr
                        }
        create_and_upload_parking_product_orders_file.delay(file_name,
                                                            data,
                                                            download_record.download_record_id,
                                                            parking_id,
                                                            request_data)

        return HttpJsonResponse({'download_record_id': download_record.download_record_id},
                                status=201)


class ParkingSuperProductOrdersView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_orders:add',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingSuperProductOrderCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        parking = ParkingHelper().get(parking_id)
        data['parking_id'] = parking_id
        product = self._build_product(data)
        order = self._build_order(data, product)
        order['where_bought'] = parking_id
        order['company_id'] = parking.company_id
        service = self._build_service(data, order)
        order = Order.objects.create(**order)
        # 购买超级包时产品短信提醒
        product_message(order)

        service['order_id'] = order.order_id.hex
        service['where_bought'] = parking_id
        service['company_id'] = parking.company_id
        service = UserService.objects.create(**service)
        send_user_service_to_parking(service, 'create')
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'service', 'income', '产品购买',
            parking.company_id, order.paid_way, order.paid_time,
            parking_id, operator,
            order.detail_info(), order.paid_amount)
        log_parking_product_order(
            request, parking, order, 'buy', 'super_timely')
        return HttpJsonResponse({
            'order_id': order.order_id.hex,
            'created_time': dtt(order.created_time),
            'service_id': service.service_id.hex,
        }, status=201)

    def _build_product(self, data):
        product = {
            'product_id': '0',
            'product_name': data['product_name'],
            'parking_id': data['parking_id'],
            'sales_count': 1,
            'car_type': data['car_type'],
            'start_date': data['start_date'],
            'end_date': data['end_date'],
            'price': data['paid_amount'],
            'refund_mode': data['refund_mode'],
            'service_durations': data['service_durations'],
            'parallel_num': data['parallel_num'],
            'comments': data['comments'],
            'quota_type': data['quota_type'],
            'quota': data['quota'],
            'quota_of_once': data['quota_of_once']
        }
        return product

    def _build_order(self, params, product):
        order = {
            'product_id': product['product_id'],
            'product_name': params['product_name'],
            'product_type': params['product_type'],
            'begin_time': params['start_date'],
            'end_time': params['end_date'],
            'car_ids': params['car_ids'],
            'use_scope': [product['parking_id']],
            'parallel_num': product['parallel_num'],
            'price': product['price'],
            'paid_amount': params['paid_amount'],
            'paid_way': 'CASH',
            'paid_time': now(),
            'order_status': 2,
            'telephone': params['telephone'],
        }
        return order

    def _build_service(self, product, order):
        product2 = copy.deepcopy(product)
        product2['start_date'] = dtt(product['start_date'])
        product2['end_date'] = dtt(product['end_date'])
        service = {
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'service_detail': product2,
            'car_type': product['car_type'],
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'telephone': order['telephone'],
            'surplus_count': 9999,
            'username': product['username'],
            'address': product['address'],
            'comments': product['comments'],
            'used_quota_detail': {
                'quota_type': product['quota_type'],
                'quota': product['quota'],
                'quota_of_once': product['quota_of_once'],
                'used_quota': {}
            }
        }
        return service


class ParkingProductOrderView(View):
    @session_required()
    def get(self, request, parking_id, product_order_id):
        if not check_perm(
                request, 'parking_products_orders:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        try:
            params = {
                'use_scope__contains': [parking_id],
                'order_id': product_order_id,
            }
            order = Order.objects.get(**params)
        except Order.DoesNotExist:
            return HttpJsonResponse(status=404)

        return HttpJsonResponse(order.detail_info())


class ParkingProductOrderRefundView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, parking_id, product_order_id):
        if not check_perm(
                request, 'parking_products_orders:refund',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingProductOrderRefundForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            params = {
                'use_scope__contains': [parking_id],
                'order_id': product_order_id,
            }
            order = Order.objects.get(**params)
        except Order.DoesNotExist:
            return HttpJsonResponse(status=404)
        if order.order_status != 2:
            return HttpJsonResponse(errors_422([
                {'resource': 'order', 'code': 'not_allow'}
            ]), status=422)

        try:
            service = UserService.objects.filter(**params).first()
        except UserService.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'user_service', 'code': 'not_found'}
            ]), status=422)

        if service.service_status != 1 or service.end_time < now():
            return HttpJsonResponse(errors_422([
                {'resource': 'user_service', 'code': 'not_allow'}
            ]), status=422)

        order.refund_amount = data['refund_amount']
        order.refund_way = 'CASH'
        order.refund_time = now()
        order.order_status = 4
        order.save()
        service.service_status = 2
        service.save()
        update_service_to_app(
            service.app_id,
            order.order_id, service.service_id,
            order.refund_amount, order.refund_way,
            order.refund_time, order.order_status, service.service_status)
        send_user_service_to_parking(service, 'delete')

        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.use_scope[0] if order.product_type != 3 else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'refund', 'expense', '产品退款',
            parking.company_id, order.refund_way, order.refund_time,
            parking_id, operator,
            order.detail_info(), order.refund_amount)

        # 记日志
        #         try:
        #             parking = Parking.objects.get(parking_id=parking_id)
        #         except Parking.DoesNotExist:
        #             return HttpJsonResponse(status=404)
        log_parking_product_refund(request, parking, order, 'refund')
        return HttpJsonResponse({
            'order_id': product_order_id,
            'service_id': service.service_id.hex,
            'refund_time': dtt(order.refund_time),
        }, status=200)


class CompanyProductOrdersView(View):
    @session_required()
    def get(self, request, company_id):
        if not check_perm(
                request, 'company_products_orders:view',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyProductOrdersQueryForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        q = Q(company_id=company_id)
        if data['section'][0]:
            q &= Q(created_time__gt=data['section'][0])
        if data['section'][1]:
            q &= Q(created_time__lt=data['section'][1])
        if data['paid_section'][0]:
            q &= Q(paid_time__gt=data['paid_section'][0])
        if data['paid_section'][1]:
            q &= Q(paid_time__lt=data['paid_section'][1])
        if data['product_type']:
            q &= Q(product_type=data['product_type'])
        if data['product_name']:
            q &= Q(product_name__contains=data['product_name'])
        if data['telephone']:
            q &= Q(telephone__contains=data['telephone'])
        if data['car_id']:
            q &= Q(car_ids__contains=[data['car_id']])
        if data['order_id']:
            q &= Q(order_id=data['order_id'])
        if data['paid_way']:
            q &= Q(paid_way=data['paid_way'])
        if data['order_status']:
            q &= Q(order_status=data['order_status'])

        if data['where_bought']:
            if data['where_bought'] == '-':
                q &= Q(where_bought='')
            else:
                q &= Q(where_bought=data['where_bought'])
        orders = Order.objects.filter(q).order_by(
            *data['order_by'])[:data['limit'] + 1]

        if data['order_by'][0].endswith('paid_time'):
            results, section_params = self._order_by_paid_time(
                data, orders)
        else:
            results, section_params = self._order_by_created_time(
                data, orders)
        resp = HttpJsonResponse(results)
        if section_params:
            params = section_params
            if data['product_type']:
                params = params + '&product_type=%s' % data['product_type']
            if data['product_name']:
                params = params + '&product_name=%s' % data['product_name']
            if data['telephone']:
                params = params + '&telephone=%s' % data['telephone']
            if data['car_id']:
                params = params + '&car_id=%s' % data['car_id']
            if data['paid_way']:
                params = params + '&paid_way=%s' % data['paid_way']
            if data['order_status']:
                params = params + '&order_status=%d' % data['order_status']
            resp['Link'] = r'<%s%s?%s>; rel="next"' % (
                get_local_host(request), request.path, params)
        return resp

    def _order_by_paid_time(self, data, orders):
        results = []
        stop_time = None
        params = None
        for order in orders[:data['limit']]:
            results.append(order.detail_info())
            stop_time = order.paid_time
        if len(orders) > data['limit']:
            params = 'limit=%d' % data['limit']
            params += '&order_by=%s' % ','.join(data['order_by'])
            if data['order_by'][0].startswith('-'):
                params += '&paid_section=%.6f,%.6f' % (
                    dtt(data['paid_section'][0]), dtt(stop_time))
            else:
                params += '&paid_section=%.6f,' % dtt(stop_time)
                if data['paid_section'][1]:
                    params += '%.6f' % dtt(data['paid_section'][1])
            if data['section'][0]:
                params += '&section=%.6f,' % dtt(
                    data['section'][0])
                if data['section'][1]:
                    params += '%.6f' % dtt(data['section'][1])
        return results, params

    def _order_by_created_time(self, data, orders):
        results = []
        stop_time = None
        params = None
        for order in orders[:data['limit']]:
            results.append(order.detail_info())
            stop_time = order.created_time
        if len(orders) > data['limit']:
            params = 'limit=%d' % data['limit']
            params += '&order_by=%s' % ','.join(data['order_by'])
            if data['order_by'][0].startswith('-'):
                params += '&section=%.6f,%.6f' % (
                    dtt(data['section'][0]), dtt(stop_time))
            else:
                params += '&section=%.6f,' % dtt(stop_time)
                if data['section'][1]:
                    params += '%.6f' % dtt(data['section'][1])
            if data['paid_section'][0]:
                params += '&paid_section=%.6f,' % dtt(
                    data['paid_section'][0])
                if data['paid_section'][1]:
                    params += '%.6f' % dtt(data['paid_section'][1])
        return results, params

    def _nextpage_link(self, request, orders):
        return ''

    @session_required()
    def post(self, request, company_id):
        if not check_perm(
                request, 'company_products_orders:add',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyProductOrderCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        data['parking_id'] = ''
        if data['where_bought']:
            data['parking_id'] = data['where_bought']
        data['company_id'] = company_id
        if data['product_type'] != 3:
            return HttpJsonResponse(errors_422([
                {'field': 'product_type', 'code': 'invalid'}
            ]), status=422)
        else:
            return self._buy_company_product(request, company_id, data)

    def _build_order(self, params, product):
        #         data = {"mobile": params['telephone']}
        #         user_id = get_htcode(data)
        order = {
            #             'user_id': user_id,
            'product_id': params['product_id'],
            'product_name': product.count_product_name,
            'product_type': params['product_type'],
            'begin_time': params['start_date'],
            'car_ids': params['car_ids'],
            'use_scope': product.use_scope,
            'parallel_num': product.parallel_num,
            'price': product.price,
            'paid_amount': params['paid_amount'],
            'paid_way': 'CASH',
            'paid_time': now(),
            'order_status': 2,
            'telephone': params['telephone'],
            'company_id': params['company_id'],
            'where_bought': params['where_bought']
        }

        if product.duration_unit == 1:
            duration = timedelta(days=product.duration)
            order['end_time'] = params['start_date'] + duration
        else:
            end_time = addmonths(params['start_date'], product.duration)
            if end_time.day < params['start_date'].day:
                end_time += timedelta(days=1)
                end_time.replace(hour=0, minute=0, second=0)
            order['end_time'] = end_time

        return order

    def _build_service(self, product, order, params):
        #         data = {"mobile": order['telephone']}
        #         user_id = get_htcode(data)
        service = {
            #             'user_id': user_id,
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'service_detail': product.detail_info(),
            'surplus_count': product.count,
            'car_type': product.car_type,
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'telephone': order['telephone'],
            'company_id': order['company_id'],
            'where_bought': params['where_bought'],
            'username': params['username'],
            'address': params['address'],
            'comments': params['comments']

        }

        return service

    def _query_already_services_count(self, order):
        return query_services_within_a_period_of_time(
            order['begin_time'], order['end_time'],
            product_id=order['product_id']).count()

    @method_decorator(atomic)
    def _buy_company_product(self, request, company_id, params):
        try:
            product = CompanyCountProduct.objects.get(
                count_product_id=params['product_id'])
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'company_count_product', 'code': 'not_found'}
            ]), status=422)
        spot = parking_spot('company_count:%s' % product.count_product_id.hex)
        if product.deleted:
            return HttpJsonResponse(status=410)
        if not product.enabled:
            return HttpJsonResponse(errors_422([
                {'resource': 'company_count_product', 'code': 'not_allow'}
            ]), status=422)
        if 2 not in product.purchase_mode:
            return HttpJsonResponse(errors_422([
                {'resource': 'company_count_product', 'code': 'not_allow'}
            ]), status=422)
        order = self._build_order(params, product)
        service = self._build_service(product, order, params)

        service_count = self._query_already_services_count(order)
        if service_count >= product.sales_count:
            return HttpJsonResponse(errors_422([
                {'resource': 'company_count_product', 'code': 'number_limits'}
            ]), status=422)

        order = Order.objects.create(**order)
        service['order_id'] = order.order_id.hex
        service['spots'] = spot
        service = UserService.objects.create(**service)

        # 发送购买短信
        product_message(order)

        send_user_service_to_parking(service, 'create')
        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.where_bought if order.where_bought else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'service', 'income', '产品购买',
            parking.company_id, order.paid_way, order.paid_time,
            parking_id, operator,
            order.detail_info(), order.paid_amount)

        # 记日志
        log_company_product_order(request, company_id, order, 'buy', params['username'])
        return HttpJsonResponse({
            'order_id': order.order_id.hex,
            'created_time': dtt(order.created_time),
            'service_id': service.service_id.hex,
            'spots': spot
        }, status=201)


class CompanyProductOrdersExportView(View):
    @session_required()
    def post(self, request, company_id):
        if not check_perm(
                request, 'company_products_orders:view',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyProductOrdersExportForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        result = {}
        time_now = time.strftime('%Y%m%d-%H%M%S', time.localtime())
        result['category'] = 'product_orders'
        file_name = '%s-%s.xlsx' % ('product_orders_list', time_now)
        result['company_id'] = company_id
        result['create_user'] = request.user.htcode
        result['file_name'] = file_name
        result['file_size'] = ''
        result['status'] = 2
        result['expire_time'] = timestamp_to_datetime(
            time.time() + 7 * 24 * 3600)
        download_record = DownloadRecord.objects.create(**result)
        request_data = {'is_superuser': request.user.is_superuser,
                        'username': request.user.htcode,
                        'remote_addr': request.remote_addr
                        }
        create_and_upload_company_product_orders_file.delay(file_name,
                                                            data,
                                                            download_record.download_record_id,
                                                            company_id,
                                                            request_data)

        return HttpJsonResponse({'download_record_id': download_record.download_record_id},
                                status=201)


class CompanyProductOrderView(View):
    @session_required()
    def get(self, request, company_id, product_order_id):
        if not check_perm(
                request, 'company_products_orders:view',
                company_id=company_id):
            return HttpResponseForbidden()
        try:
            order = Order.objects.get(order_id=product_order_id)
        except Order.DoesNotExist:
            return HttpJsonResponse(status=404)

        return HttpJsonResponse(order.detail_info())


class CompanyProductOrderRefundView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, company_id, product_order_id):
        if not check_perm(
                request, 'company_products_orders:refund',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingProductOrderRefundForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            order = Order.objects.get(order_id=product_order_id)
        except Order.DoesNotExist:
            return HttpJsonResponse(status=404)
        if order.order_status != 2:
            return HttpJsonResponse(errors_422([
                {'resource': 'order', 'code': 'not_allow'}
            ]), status=422)

        try:
            service = UserService.objects.filter(
                order_id=product_order_id).first()
        except UserService.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'user_service', 'code': 'not_found'}
            ]), status=422)

        if service.service_status != 1 or service.end_time < now():
            return HttpJsonResponse(errors_422([
                {'resource': 'user_service', 'code': 'not_allow'}
            ]), status=422)

        order.refund_amount = data['refund_amount']
        order.refund_way = 'CASH'
        order.refund_time = now()
        order.order_status = 4
        order.save()
        service.service_status = 2
        service.save()
        update_service_to_app(
            service.app_id,
            order.order_id, service.service_id,
            order.refund_amount, order.refund_way,
            order.refund_time, order.order_status, service.service_status)
        send_user_service_to_parking(service, 'delete')

        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.where_bought if order.where_bought else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'refund', 'expense', '产品退款',
            parking.company_id, order.refund_way, order.refund_time,
            parking_id, operator,
            order.detail_info(), order.refund_amount)
        # 记日志
        log_company_product_refund(request, company_id, order, 'refund')
        return HttpJsonResponse({
            'order_id': product_order_id,
            'service_id': service.service_id.hex,
            'refund_time': dtt(order.refund_time),
        }, status=200)


class ParkingUserServicesView(View):
    @session_required()
    def get(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_userservices:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingUserServicesQueryForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        query, params = self._build_q(parking_id, data)
        services = UserService.objects.raw(query, params)[:data['limit'] + 1]

        has_next = False
        if len(services) == data['limit'] + 1:
            has_next = True

        stop_stamp = data['section'][1] if data['section'][1] else now()
        services = services[:data['limit']]
        results = []
        for service in services:
            results.append(service.detail_info())
            stop_stamp = service.created_time
        stop_stamp = dtt(stop_stamp)
        start_stamp = dtt(data['section'][0]) if data['section'][0] else 0
        resp = HttpJsonResponse(results)
        if has_next:
            params = 'section=%.6f,%.6f' % (
                start_stamp, stop_stamp)
            if data['limit']:
                params = params + '&limit=%d' % data['limit']
            if data['begin_section'][0] or data['begin_section'][1]:
                params = params + \
                         '&begin_section=%.6f,%.6f' % (
                             dtt(data['begin_section'][0]) if data[
                                 'begin_section'][0] else 0,
                             dtt(data['begin_section'][1]) if data['begin_section'][1] else 0)
            if data['product_type']:
                params = params + \
                         '&product_type=%s' % ','.join(data['product_type'])
            if data['product_name']:
                params = params + '&product_name=%s' % quote(data["product_name"])
            if data['telephone']:
                params = params + '&telephone=%s' % data['telephone']
            if data['car_id']:
                params = params + '&car_id=%s' % urllib.parse.quote(data['car_id'])
            if data['region_id']:
                params = params + '&region_id=%s' % data['region_id']
            if data['status']:
                params = params + '&status=%d' % data['status']
            if data['order_by']:
                params = params + '&order_by=%s' % (','.join(data['order_by']))
            if data['username']:
                params = params + '&username=%s' % quote(data["username"])
            resp['Link'] = r'<%s%s?%s>; rel="next"' % (
                get_local_host(request), request.path, params)
        return resp

    def _nextpage_link(self, request, orders):
        return ''

    def _build_q(self, parking_id, data):
        data['product_ids'] = self._rel_product(parking_id, data)
        query = 'select * from product_userservice where use_scope @> %s'
        params = ['{%s}' % parking_id]
        if data['section'][0]:
            query += " and created_time > %s"
            params.append(data['section'][0])
        if data['section'][1]:
            query += " and created_time < %s"
            params.append(data['section'][1])
        if data['begin_section'][0]:
            query += " and end_time > %s"
            params.append(data['begin_section'][0])
        if data['begin_section'][1]:
            query += " and begin_time < %s"
            params.append(data['begin_section'][1])
        if data['region_id']:
            if data['product_ids']:
                query += " and product_id in (%s)" % ','.join(data['product_ids'])
            else:
                query += " and product_id in ('')"
        if data['product_type']:
            query += " and product_type in (%s)" % ','.join(data['product_type'])
        if data['product_name']:
            query += " and product_name like %s"
            params.append('%%%s%%' % data['product_name'])
        if data['telephone']:
            query += "  and telephone like %s "
            params.append('%%%s%%' % data['telephone'])
        if data['car_id']:
            query += " and array_to_string(car_ids, ',') like %s"
            params.append('%%%s%%' % data['car_id'])
        if data['username']:
            query += " and username like %s"
            params.append('%%%s%%' % data['username'])
        current_time = now()
        if data['status'] == 0:
            pass
        elif data['status'] == 1:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time > %s"
            params.append(current_time)
        elif data['status'] == 2:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time <= %s"
            params.append(current_time)
        elif data['status'] == 3:
            query += " and service_status = 1"
            query += " and surplus_count = 0"
        elif data['status'] == 4:
            query += " and service_status = 2"

        query += ' order by'
        for column in data['order_by']:
            if column.startswith('-'):
                query += " %s desc," % column[1:]
            else:
                query += " %s," % column
        query = query[:-1]

        return query, params

    def _rel_product(self, parking_id, data):
        product_ids, p_types = [], data['product_type']
        if not data['region_id']:
            return product_ids
        stop_areas = StopArea.objects.filter(
            parking_id=parking_id,
            region_id=data['region_id'],
            is_deleted=False
        )
        for stop_area in stop_areas:
            subtype = stop_area.driver_subtype.detail_info()
            code = subtype['code'].split(':')
            if code[0] == 'timely' and ('1' in p_types or '4' in p_types):
                product_id = code[1] if code[1] != 'super' else '0'
            elif code[0] == 'count' and ('2' in p_types):
                product_id = code[1]
            elif code[0] == 'company_count' and ('3' in p_types):
                product_id = code[1]
            else:
                continue
            if product_id not in product_ids:
                product_ids.append(product_id)
        product_ids = [str('\'' + _ + '\'') for _ in product_ids]
        return product_ids


class ParkingUserServicesExportView(View):
    @session_required()
    @method_decorator(atomic)
    def get(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_userservices:export',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingUserServicesQueryForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        query, params = self._build_q(parking_id, data)
        services = UserService.objects.raw(query, params)
        datas = OrderedDict()
        if len([_ for _ in data['product_type'] if _ not in ['1', '4']]) == 0:
            export_data = [
                ['手机号', '姓名', '地址', '车牌号', '产品名称', '剩余天数', '购买时间', '开始时间', '到期时间', '实付', '备注']]
        elif len([_ for _ in data['product_type'] if _ not in ['2']]) == 0:
            export_data = [
                ['手机号', '姓名', '地址', '车牌号', '产品名称', '剩余次数', '购买时间', '开始时间', '到期时间', '实付', '备注']]
        for s in services:
            if s.product_type in [1, 4]:
                export_data.append(self._parking_timely_userservice_string(s))
            if s.product_type == 2:
                export_data.append(self._parking_count_userservice_string(s))
        datas.update({"Sheet": export_data})
        bytes_io = BytesIO()
        save_data(bytes_io, datas, file_type='xlsx')
        bytes_io.seek(0)
        response = FileResponse(
            bytes_io,
            status=200,
            content_type='application/x-xls')
        f = 'parking_userservices-%s.xlsx' % now().strftime('%Y%m%d-%H%M%S')
        response['Content-Disposition'] = ('attachment; filename=%s' % f)
        log_parking_userservice_export(
            request, parking_id, len(export_data) - 1, 'export')
        return response

    @session_required()
    @method_decorator(atomic)
    def post(self, request, parking_id):
        is_valid, data = validate_form(
            ParkingUserServicesQueryForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        logger.exception('*****data:%s***' % data)
        result = {}
        time_now = time.strftime('%Y%m%d-%H%M%S', time.localtime())
        if len([_ for _ in data['product_type'] if _ not in ['1', '4']]) == 0:
            result['category'] = 'product_time'
        elif len([_ for _ in data['product_type'] if _ not in ['2']]) == 0:
            result['category'] = 'product_frequency'
        file_name = '%s-%s.xlsx' % ('parking_userservices', time_now)
        result['parking_id'] = parking_id
        result['create_user'] = request.user.htcode
        result['file_name'] = file_name
        result['file_size'] = ''
        result['status'] = 2
        result['expire_time'] = timestamp_to_datetime(
            time.time() + 7 * 24 * 3600)
        download_record = DownloadRecord.objects.create(**result)
        request_data = {
            'is_superuser': request.user.is_superuser,
            'username': request.user.htcode,
            'remote_addr': request.remote_addr
        }
        if data['begin_section'][0]:
            data['begin_section'][0] = dtt(data['begin_section'][0])
        if data['begin_section'][1]:
            data['begin_section'][1] = dtt(data['begin_section'][1])
        if data['section'][0]:
            data['section'][0] = dtt(data['section'][0])
        if data['section'][1]:
            data['section'][1] = dtt(data['section'][1])
        create_and_upload_parking_userservices_file.delay(
            file_name,
            data,
            download_record.download_record_id,
            parking_id,
            request_data
        )

        return HttpJsonResponse({'download_record_id': download_record.download_record_id},
                                status=201)

    def _parking_timely_userservice_string(self, s):
        surplus_days = self._timely_surplus_days(s.begin_time, s.end_time)
        if not s.order_id:
            pay_amount = None
        else:
            order = Order.objects.get(order_id=s.order_id)
            pay_amount = order.paid_amount
        if s.car_ids is None:
            car_ids = ''
        else:
            car_ids = ';'.join(s.car_ids)
        if order.paid_time is None:
            pay_time = None
        else:
            pay_time = (
                    order.paid_time + datetime.timedelta(hours=8)
            ).strftime('%Y/%m/%d')

        return [
            s.telephone,
            s.username,
            s.address,
            car_ids,
            s.product_name,
            surplus_days,
            pay_time,
            (s.begin_time + datetime.timedelta(hours=8)).strftime('%Y/%m/%d '),
            (ttd((dtt(s.end_time) - 1))).strftime('%Y/%m/%d '),
            pay_amount,
            s.comments
        ]

    def _parking_count_userservice_string(self, s):
        order = Order.objects.get(order_id=s.order_id)
        if s.car_ids is None:
            car_ids = ''
        else:
            car_ids = ','.join(s.car_ids)
        if order.paid_time is None:
            paid_time = None
        else:
            paid_time = (order.paid_time + datetime.timedelta(hours=8)
                         ).strftime('%Y/%m/%d')
        return [
            s.telephone,
            s.username,
            s.address,
            car_ids,
            s.product_name,
            s.surplus_count,
            paid_time,
            (s.begin_time + datetime.timedelta(hours=8)).strftime('%Y/%m/%d '),
            (ttd((dtt(s.end_time) - 1))).strftime('%Y/%m/%d '),
            order.paid_amount,
            s.comments
        ]

    def _timely_surplus_days(self, begin_time, end_time):
        now = time.time()
        if dtt(begin_time) > now:
            return int((dtt(end_time) - dtt(begin_time)) / (3600 * 24))
        if dtt(begin_time) < now and now < dtt(end_time):
            return int((dtt(end_time) - now) / (3600 * 24))
        if now > dtt(end_time):
            return 0

    def _build_q(self, parking_id, data):
        query = 'select * from product_userservice where use_scope @> %s'
        params = ['{%s}' % parking_id]
        if data['section'][0]:
            query += " and created_time > %s"
            params.append(data['section'][0])
        if data['section'][1]:
            query += " and created_time < %s"
            params.append(data['section'][1])
        if data['begin_section'][0]:
            query += " and end_time >= %s"
            params.append(data['begin_section'][0])
        if data['begin_section'][1]:
            query += " and begin_time <= %s"
            params.append(data['begin_section'][1])
        if data['product_type']:
            data['product_type'] = [str(_) for _ in data['product_type']]
            query += " and product_type in (%s)" % ','.join(
                data['product_type'])
        # params.append(data['product_type'])
        if data['product_name']:
            query += " and product_name like %s"
            params.append('%%%s%%' % data['product_name'])
        if data['telephone']:
            query += "  and telephone like %s "
            params.append('%%%s%%' % data['telephone'])
        if data['car_id']:
            query += " and array_to_string(car_ids, ',') like %s"
            params.append('%%%s%%' % data['car_id'])
        if data['username']:
            query += " and username like %s"
            params.append('%%%s%%' % data['username'])

        current_time = now()
        if data['status'] == 0:
            pass
        elif data['status'] == 1:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time > %s"
            params.append(current_time)
        elif data['status'] == 2:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time <= %s"
            params.append(current_time)
        elif data['status'] == 3:
            query += " and service_status = 1"
            query += " and surplus_count = 0"
        elif data['status'] == 4:
            query += " and service_status = 2"

        query += ' order by'
        for column in data['order_by']:
            if column.startswith('-'):
                query += " %s desc," % column[1:]
            else:
                query += " %s," % column
        query = query[:-1]

        return query, params


class ParkingUserServicesImportView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_userservices:import',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingUserServicesImportForm, request.POST)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        product_type = data['product_type']
        try:
            if product_type == 1:
                product = ParkingTimelyProduct.objects.get(
                    timely_product_id=data['product_id'],
                    parking_id=parking_id)
                product_name = product.timely_product_name
            else:
                product = ParkingCountProduct.objects.get(
                    count_product_id=data['product_id'], parking_id=parking_id)
                product_name = product.count_product_name
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'ParkingTimelyProduct', 'code': 'not_found'}
            ]), status=422)
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'ParkingCountProduct', 'code': 'not_found'}
            ]), status=422)
        try:
            import_file = request.FILES['file']
            sheet = pyexcel.get_sheet(
                file_type='xlsx', file_content=import_file.file.read())
            if len(sheet.array) <= 1:
                return HttpJsonResponse(errors_422([
                    {"resource": "file", "code": "missing"},
                ]), status=422)
            elif len(sheet.array) > 1001:
                return HttpJsonResponse(errors_422([
                    {"resource": "file", "code": "number_limits"},
                ]), status=422)
        except:
            return HttpJsonResponse(errors_422([
                {"resource": "file", "code": "invalid"},
            ]), status=422)

        try:
            company_id = ParkingHelper.get(parking_id).company_id
            line_num = 0
            orders, services = [], []
            product_info = product.detail_info()
            for index, record in enumerate(sheet.array[1:1001]):
                if not record[0]:
                    return HttpJsonResponse(errors_422([
                        {'resource': 'data', 'code': 'invalid'},
                    ]), status=422)
                if product_type == 1:
                    record.insert(3, 9999)
                logger.exception('record:%s' % record)
                line_num += 1

                if not is_vaild_date(record[4]) or not is_vaild_date(record[5]) or not is_vaild_date(record[6]):
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)

                if not matching_telephone_simpleness(str(record[1]).replace(' ', '')):
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)

                if not matching_many_car_id(str(record[2])):
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)

                if record[5] > record[6]:
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)
                order = self._build_order(product, record, product_type)
                order['company_id'] = company_id
                order['where_bought'] = parking_id
                orders.append(Order(**order))
                service = self._build_service(product, order, record)
                service['company_id'] = company_id
                service['where_bought'] = parking_id
                service['service_detail'] = product_info
                services.append(UserService(**service))
            Order.objects.bulk_create(orders)
            UserService.objects.bulk_create(services)

            for _ in services:
                send_user_service_to_parking(_, 'import')
            try:
                parking = Parking.objects.get(parking_id=parking_id)
            except:
                return HttpJsonResponse(status=404)
            log_parking_userservice_import(
                request, parking, len(sheet.array), 'import', product_name)
            return HttpJsonResponse(status=204)
        except Exception as e:
            logger.exception(e)
            return HttpJsonResponse(errors_422([
                {'resource': 'data', 'code': 'invalid'},
            ]), status=422)

    def _build_order(self, product, record, product_type):
        order = {
            'order_id': uuid.uuid1().hex,
            'product_type': product_type,
            'use_scope': [product.parking_id],
            'parallel_num': product.parallel_num,
            'order_status': 2,
            'price': product.price,
            'paid_way': 'CASH',
            'comments': '',
            'telephone': str(record[1]).replace(' ', ''),
            'car_ids': str(record[2]).replace(' ', '').upper().split(';'),
            'paid_time': ttd(dtt(record[4]) + get_micronseconds()),
            'begin_time': record[5],
            'end_time': record[6] + timedelta(days=1),
            'paid_amount': record[7],
        }
        if product_type == 1:
            order['product_id'] = product.timely_product_id.hex
            order['product_name'] = product.timely_product_name
        else:
            order['product_id'] = product.count_product_id.hex
            order['product_name'] = product.count_product_name
        return order

    def _build_service(self, product, order, record):
        #         data = {"mobile": order['telephone']}
        #         user_id = get_htcode(data)
        service = {
            'service_id': uuid.uuid1().hex,
            #             'user_id': user_id,
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'surplus_count': record[3],
            'order_id': order['order_id'],
            'car_type': product.car_type,
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'comments': str(record[9]).strip(),
            'telephone': order['telephone'],
            'username': str(record[0]).replace(' ', ''),
            'address': str(record[8]).strip()
        }
        return service


class ParkingUserServiceView(View):
    @session_required()
    def get(self, request, parking_id, service_id):
        if not check_perm(
                request, 'parking_products_userservices:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        try:
            params = {
                'use_scope__contains': [parking_id],
                'service_id': 'service_id',
            }
            service = UserService.objects.get(**params)
        except UserService.DoesNotExist:
            return HttpJsonResponse(status=404)

        return HttpJsonResponse(service.detail_info())


class ParkingUserServiceCarsView(View):
    @session_required()
    @method_decorator(atomic)
    def patch(self, request, parking_id, service_id):
        if not check_perm(
                request, 'parking_products_userservices:update',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingUserServicePatchCarsForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            service = UserService.objects.get(
                use_scope__contains=[parking_id],
                service_id=service_id, service_status=1)
        except UserService.DoesNotExist:
            return HttpJsonResponse(status=404)
        old_service = service.detail_info()
        #         datas = {
        #             "uucode": service.uucode,
        #             "begin_time": service.begin_time,
        #             "end_time": service.end_time,
        #             "car_ids": service.car_ids
        #         }

        service.car_ids = data['car_ids']

        if data['telephone']:
            service.telephone = data['telephone']
        if data['username']:
            service.username = data['username']
        if data['address']:
            service.address = data['address']
        if data['comments']:
            service.comments = data['comments']

        service.save()
        send_user_service_to_parking(service, 'update')

        # 记日志
        try:
            parking = Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        log_parking_userservices_car(
            request, parking, old_service, service, 'update')
        return HttpJsonResponse(status=204)


class ParkingUserServiceUserView(View):
    @session_required()
    @method_decorator(atomic)
    def put(self, request, parking_id, service_id):
        if not check_perm(
                request, 'parking_products_userservices:update',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingUserServicePatchUserForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            service = UserService.objects.get(
                use_scope__contains=[parking_id],
                service_id=service_id, service_status=1)
        except UserService.DoesNotExist:
            return HttpJsonResponse(status=404)
        old_service = service.detail_info()
        service.telephone = data['telephone']
        service.username = data['username']
        service.address = data['address']
        service.comments = data['comments']
        service.save()
        send_user_service_to_parking(service, 'update')
        # 记日志
        # try:
        #     parking = Parking.objects.get(parking_id=parking_id)
        # except Parking.DoesNotExist:
        #     return HttpJsonResponse(status=404)
        # log_parking_userservices_car(
        #     request, parking, old_service, service, 'update')
        return HttpJsonResponse(status=204)


class CompanyUserServicesView(View):
    @session_required()
    def get(self, request, company_id):
        if not check_perm(
                request, 'company_products_userservices:view',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyUserServicesQueryForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        parkings = Parking.objects.filter(company_id=company_id)
        parking_ids = [_.parking_id for _ in parkings]
        parking_ids = ','.join(parking_ids)
        query, params = self._build_q(parking_ids, data)
        services = UserService.objects.raw(query, params)[:data['limit'] + 1]

        has_next = False
        if len(services) == data['limit'] + 1:
            has_next = True

        stop_stamp = data['section'][1] if data['section'][1] else now()
        services = services[:data['limit']]
        results = []
        for service in services:
            results.append(service.detail_info())
            stop_stamp = service.created_time
        stop_stamp = dtt(stop_stamp)
        start_stamp = dtt(data['section'][0]) if data['section'][0] else 0
        resp = HttpJsonResponse(results)
        if has_next:
            params = 'section=%.6f,%.6f' % (
                start_stamp, stop_stamp)
            if data['limit']:
                params = params + '&limit=%d' % data['limit']
            if data['begin_section'][0] or data['begin_section'][1]:
                params = params + \
                         '&begin_section=%.6f,%.6f' % (
                             dtt(data['begin_section'][0]) if data[
                                 'begin_section'][0] else 0,
                             dtt(data['begin_section'][1]) if data['begin_section'][1] else 0)
            if data['product_type']:
                params = params + \
                         '&product_type=%s' % ','.join(data['product_type'])
            if data['product_name']:
                params = params + '&product_name=%s' % data['product_name']
            if data['telephone']:
                params = params + '&telephone=%s' % data['telephone']
            if data['car_id']:
                params = params + '&car_id=%s' % urllib.parse.quote(data['car_id'])
            if data['status']:
                params = params + '&status=%d' % data['status']
            if data['order_by']:
                params = params + '&order_by=%s' % ','.join(data['order_by'])
            if data['parking_id']:
                params = params + '&parking_id=%s' % data['parking_id']
            if data['username']:
                params = params + '&username=%s' % quote(data['username'])
            resp['Link'] = r'<%s%s?%s>; rel="next"' % (
                get_local_host(request), request.path, params)
        return resp

    def _nextpage_link(self, request, orders):
        return ''

    def _build_q(self, parking_ids, data):
        product_ids = None
        if data['parking_id']:
            product_ids = self._rel_product(data['parking_id'], data)
            query = 'select * from product_userservice where use_scope @> %s'
            params = ['{%s}' % data['parking_id']]
        else:
            query = 'select * from product_userservice where 1=1'
            query += " and use_scope <@ '{%s}'" % parking_ids
            params = []
        if data['section'][0]:
            query += " and created_time > %s"
            params.append(data['section'][0])
        if data['section'][1]:
            query += " and created_time < %s"
            params.append(data['section'][1])
        if data['begin_section'][0]:
            query += " and end_time > %s"
            params.append(data['begin_section'][0])
        if data['begin_section'][1]:
            query += " and begin_time < %s"
            params.append(data['begin_section'][1])
        if data['region_id']:
            if product_ids:
                query += " and product_id in (%s)" % ','.join(product_ids)
            else:
                query += " and product_id in ('')"
        if data['product_type']:
            query += " and product_type in (%s)" % ','.join(data['product_type'])
        if data['product_name']:
            query += " and product_name like %s"
            params.append('%%%s%%' % data['product_name'])
        if data['telephone']:
            query += "  and telephone like %s "
            params.append('%%%s%%' % data['telephone'])
        if data['car_id']:
            query += " and array_to_string(car_ids, ',') like %s"
            params.append('%%%s%%' % data['car_id'])
        if data['username']:
            query += " and username like %s"
            params.append('%%%s%%' % data['username'])

        current_time = now()
        if data['status'] == 0:
            pass
        elif data['status'] == 1:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time > %s"
            params.append(current_time)
        elif data['status'] == 2:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time <= %s"
            params.append(current_time)
        elif data['status'] == 3:
            query += " and service_status = 1"
            query += " and surplus_count = 0"
        elif data['status'] == 4:
            query += " and service_status = 2"

        query += ' order by'
        for column in data['order_by']:
            if column.startswith('-'):
                query += " %s desc," % column[1:]
            else:
                query += " %s," % column
        query = query[:-1]

        return query, params

    def _rel_product(self, parking_id, data):
        product_ids, p_types = [], data['product_type']
        if not data['region_id']:
            return product_ids
        stop_areas = StopArea.objects.filter(
            parking_id=parking_id,
            region_id=data['region_id'],
            is_deleted=False
        )
        for stop_area in stop_areas:
            subtype = stop_area.driver_subtype.detail_info()
            code = subtype['code'].split(':')
            if code[0] == 'timely' and ('1' in p_types or '4' in p_types):
                product_id = code[1] if code[1] != 'super' else '0'
            elif code[0] == 'count' and ('2' in p_types):
                product_id = code[1]
            elif code[0] == 'company_count' and ('3' in p_types):
                product_id = code[1]
            else:
                continue
            if product_id not in product_ids:
                product_ids.append(product_id)
        product_ids = [str('\'' + _ + '\'') for _ in product_ids]
        return product_ids


class CompanyUserServicesImportView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, company_id):
        if not check_perm(
                request, 'company_products_userservices:import',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyUserServicesImportForm, request.POST)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        try:
            product = CompanyCountProduct.objects.get(
                count_product_id=data['product_id'], company_id=company_id)
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'CompanyCountProduct', 'code': 'not_found'}
            ]), status=422)
        try:
            import_file = request.FILES['file']
            sheet = pyexcel.get_sheet(
                file_type='xlsx', file_content=import_file.file.read())
            if len(sheet.array) <= 1:
                return HttpJsonResponse(errors_422([
                    {"resource": "file", "code": "missing"},
                ]), status=422)
            elif len(sheet.array) > 1001:
                return HttpJsonResponse(errors_422([
                    {"resource": "file", "code": "number_limits"},
                ]), status=422)
        except:
            return HttpJsonResponse(errors_422([
                {"resource": "file", "code": "invalid"},
            ]), status=422)

        try:
            line_num = 0
            orders, services = [], []
            product_info = product.detail_info()
            for index, record in enumerate(sheet.array[1:1001]):
                if not record[0]:
                    break
                line_num += 1

                if not is_vaild_date(record[4]) or not is_vaild_date(record[5]) or not is_vaild_date(record[6]):
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)

                if not matching_telephone_simpleness(str(record[1]).replace(' ', '')):
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)

                if not matching_many_car_id(str(record[2])):
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)

                if record[5] > record[6]:
                    return HttpJsonResponse(
                        {"message": "Validation Failed", "errors": [
                            {"resources": "userservice",
                             "line": line_num, "code": "error_time"}
                        ]}, status=422)
                order = self._build_order(product, record)
                order['company_id'] = company_id
                order['where_bought'] = data['parking_id']
                orders.append(Order(**order))
                service = self._build_service(product, order, record)
                service['company_id'] = company_id
                service['where_bought'] = data['parking_id']
                service['service_detail'] = product_info
                services.append(UserService(**service))
            Order.objects.bulk_create(orders)
            UserService.objects.bulk_create(services)
            for _ in services:
                send_user_service_to_parking(_, 'import')
            log_company_userservice_import(
                request, company_id, len(sheet.array), 'import', '企业包次产品')
            return HttpJsonResponse(status=204)
        except Exception as e:
            logger.exception(e)
            return HttpJsonResponse(errors_422([
                {'resource': 'data', 'code': 'invalid'},
            ]), status=422)

    def _build_order(self, product, record):
        #         if isinstance(record[5], str):
        #             record[5] = datetime.datetime.strptime(record[5], '%Y-%m-%d')
        order = {
            'order_id': uuid.uuid1().hex,
            'product_type': 3,
            'use_scope': product.use_scope,
            'parallel_num': product.parallel_num,
            'order_status': 2,
            'price': product.price,
            'paid_way': 'CASH',
            'comments': '',
            'product_id': product.count_product_id.hex,
            'product_name': product.count_product_name,
            'telephone': str(record[1]).replace(' ', ''),
            'car_ids': str(record[2]).replace(' ', '').upper().split(';'),
            'paid_time': ttd(dtt(record[4]) + get_micronseconds()),
            'begin_time': record[5],
            'end_time': record[6] + timedelta(days=1),
            'paid_amount': record[7],
        }
        return order

    def _build_service(self, product, order, record):
        #         data = {"mobile": order['telephone']}
        #         user_id = get_htcode(data)
        service = {
            'service_id': uuid.uuid1().hex,
            #             'user_id': user_id,
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'surplus_count': record[3],
            'order_id': order['order_id'],
            'car_type': product.car_type,
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'comments': str(record[9]).strip(),
            'telephone': order['telephone'],
            'username': str(record[0]).replace(' ', ''),
            'address': str(record[8]).strip()
        }
        return service


class CompanyUserServicesExportView(View):
    @session_required()
    @method_decorator(atomic)
    def get(self, request, company_id):
        if not check_perm(
                request, 'company_products_userservices:import',
                parking_id=company_id):
            return HttpResponseForbidden()
        flag, data = validate_form(CompanyUserServicesQueryForm, request.GET)
        if not flag:
            return HttpJsonResponse(({
                "message": "Validation Failed", "errors": data
            }), status=422)
        parkings = Parking.objects.filter(company_id=company_id)
        parking_ids = [_.parking_id for _ in parkings]
        parking_ids = ','.join(parking_ids)
        query, params = self._build_q(parking_ids, data)
        services = UserService.objects.raw(query, params)
        datas = OrderedDict()
        export_data = [
            ['手机号', '姓名', '地址', '车牌号', '产品名称', '剩余次数', '购买时间', '开始时间', '到期时间', '实付', '备注']]
        for s in services:
            export_data.append(self._company_userservice_string(s))
        datas.update({"Sheet": export_data})
        bytes_io = BytesIO()
        save_data(bytes_io, datas, file_type='xlsx')
        bytes_io.seek(0)
        response = FileResponse(
            bytes_io,
            status=200,
            content_type='application/x-xls')

        f = 'company_userservices-%s.xlsx' % now().strftime('%Y%m%d-%H%M%S')
        response['Content-Disposition'] = ('attachment; filename=%s' % f)
        log_company_userservice_export(
            request, company_id, len(export_data) - 1, 'export')
        return response

    @session_required()
    @method_decorator(atomic)
    def post(self, request, company_id):
        flag, data = validate_form(
            CompanyUserServicesQueryForm, request.jsondata)
        if not flag:
            return HttpJsonResponse(({
                "message": "Validation Failed", "errors": data
            }), status=422)
        result = {}
        time_now = time.strftime('%Y%m%d-%H%M%S', time.localtime())
        result['category'] = 'product_frequency'
        file_name = '%s-%s.xlsx' % ('parking_userservices', time_now)
        result['company_id'] = company_id
        result['create_user'] = request.user.htcode
        result['file_name'] = file_name
        result['file_size'] = ''
        result['status'] = 2
        result['expire_time'] = timestamp_to_datetime(
            time.time() + 7 * 24 * 3600)
        download_record = DownloadRecord.objects.create(**result)
        request_data = {'is_superuser': request.user.is_superuser,
                        'username': request.user.htcode,
                        'remote_addr': request.remote_addr
                        }
        if data['begin_section'][0]:
            data['begin_section'][0] = dtt(data['begin_section'][0])
        if data['begin_section'][1]:
            data['begin_section'][1] = dtt(data['begin_section'][1])
        if data['section'][0]:
            data['section'][0] = dtt(data['section'][0])
        if data['section'][1]:
            data['section'][1] = dtt(data['section'][1])
        create_and_upload_company_userservices_file.delay(file_name,
                                                          data,
                                                          download_record.download_record_id,
                                                          company_id,
                                                          request_data)
        return HttpJsonResponse({'download_record_id': download_record.download_record_id},
                                status=201)

    def _company_userservice_string(self, s):
        if not s.order_id:
            pay_amount = None
        else:
            order = Order.objects.get(order_id=s.order_id)
            pay_amount = order.paid_amount
        if s.car_ids is None:
            car_ids = ''
        else:
            car_ids = ';'.join(s.car_ids)
        if order.paid_time is None:
            pay_time = None
        else:
            pay_time = (
                    order.paid_time + datetime.timedelta(hours=8)
            ).strftime('%Y/%m/%d')
        return [
            s.telephone,
            s.username,
            s.address,
            car_ids,
            s.product_name,
            s.surplus_count,
            pay_time,
            (s.begin_time + datetime.timedelta(hours=8)).strftime('%Y/%m/%d '),
            #             ttd(dtt((s.end_time + datetime.timedelta(hours=8))) -
            #              1).strftime('%Y/%m/%d '),
            (ttd((dtt(s.end_time) - 1))).strftime('%Y/%m/%d '),
            pay_amount,
            s.comments
        ]

    def _build_q(self, parking_ids, data):
        product_ids = None
        if data['parking_id']:
            product_ids = self._rel_product(data['parking_id'], data)
            query = 'select * from product_userservice where use_scope @> %s'
            params = ['{%s}' % data['parking_id']]
        else:
            query = 'select * from product_userservice where 1=1'
            query += " and use_scope <@ '{%s}'" % parking_ids
            params = []
        if data['section'][0]:
            query += " and created_time > %s"
            params.append(data['section'][0])
        if data['section'][1]:
            query += " and created_time < %s"
            params.append(data['section'][1])
        if data['begin_section'][0]:
            query += " and end_time > %s"
            params.append(data['begin_section'][0])
        if data['begin_section'][1]:
            query += " and begin_time < %s"
            params.append(data['begin_section'][1])
        if data['region_id']:
            if product_ids:
                query += " and product_id in (%s)" % ','.join(product_ids)
            else:
                query += " and product_id in ('')"
        if data['product_type']:
            query += " and product_type in (%s)" % ','.join(data['product_type'])
        if data['product_name']:
            query += " and product_name like %s"
            params.append('%%%s%%' % data['product_name'])
        if data['telephone']:
            query += "  and telephone like %s "
            params.append('%%%s%%' % data['telephone'])
        if data['car_id']:
            query += " and array_to_string(car_ids, ',') like %s"
            params.append('%%%s%%' % data['car_id'])
        if data['username']:
            query += " and username like %s"
            params.append('%%%s%%' % data['username'])

        current_time = now()
        if data['status'] == 0:
            pass
        elif data['status'] == 1:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time > %s"
            params.append(current_time)
        elif data['status'] == 2:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time <= %s"
            params.append(current_time)
        elif data['status'] == 3:
            query += " and service_status = 1"
            query += " and surplus_count = 0"
        elif data['status'] == 4:
            query += " and service_status = 2"

        query += ' order by'
        for column in data['order_by']:
            if column.startswith('-'):
                query += " %s desc," % column[1:]
            else:
                query += " %s," % column
        query = query[:-1]

        return query, params


class CompanyUserServiceView(View):
    @session_required()
    def get(self, request, company_id, service_id):
        if not check_perm(
                request, 'company_products_userservices:view',
                company_id=company_id):
            return HttpResponseForbidden()
        try:
            service = UserService.objects.get(service_id=service_id)
        except UserService.DoesNotExist:
            return HttpJsonResponse(status=404)

        return HttpJsonResponse(service.detail_info())


class CompanyUserServiceCarsView(View):
    @session_required()
    @method_decorator(atomic)
    def patch(self, request, company_id, service_id):
        if not check_perm(
                request, 'company_products_userservices:update',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyUserServicePatchCarsForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            service = UserService.objects.get(
                service_id=service_id, service_status=1)
        except UserService.DoesNotExist:
            return HttpJsonResponse(status=404)
        old_service = service.detail_info()
        #         datas = {
        #             "uucode": service.uucode,
        #             "begin_time": service.begin_time,
        #             "end_time": service.end_time,
        #             "car_ids": service.car_ids
        #         }
        service.car_ids = data['car_ids']

        if data['telephone']:
            service.telephone = data['telephone']
        if data['username']:
            service.username = data['username']
        if data['address']:
            service.address = data['address']
        if data['comments']:
            service.comments = data['comments']

        service.save()
        send_user_service_to_parking(service, 'update')
        # 记日志
        log_company_userservices_car(
            request, company_id, old_service, service, 'update')
        return HttpJsonResponse(status=204)


class CompanyUserServiceUserView(View):
    @session_required()
    @method_decorator(atomic)
    def put(self, request, company_id, service_id):
        if not check_perm(
                request, 'company_products_userservices:update',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyUserServicePatchUserForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        try:
            service = UserService.objects.get(
                service_id=service_id, service_status=1)
        except UserService.DoesNotExist:
            return HttpJsonResponse(status=404)
        old_service = service.detail_info()
        service.telephone = data['telephone']
        service.username = data['username']
        service.address = data['address']
        service.comments = data['comments']
        service.save()
        send_user_service_to_parking(service, 'update')
        # log_company_userservices_car(
        #     request, company_id, old_service, service, 'update')
        return HttpJsonResponse(status=204)


class CompanyCountProductSpaceCountView(View):
    @session_required()
    def get(self, request, company_id, count_product_id):
        try:
            Company.objects.get(company_id=company_id)
        except Company.DoesNotExist:
            return HttpJsonResponse(status=404)
        try:
            product = CompanyCountProduct.objects.get(
                count_product_id=count_product_id)
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        code = 'company_count:%s' % product.count_product_id.hex
        count = get_sales_count(code)
        product.sales_count = count
        product.save()
        return HttpJsonResponse({'space_count': count})


class ParkingTimelyProductsSpaceCountView(View):
    @session_required()
    def get(self, request, parking_id, timely_product_id):
        try:
            Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        try:
            product = ParkingTimelyProduct.objects.get(
                timely_product_id=timely_product_id)
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        code = 'timely:%s' % product.timely_product_id.hex
        count = get_sales_count(code)
        product.sales_count = count
        product.save()
        return HttpJsonResponse({'space_count': count})


class ParkingCountProductsSpaceCountView(View):
    @session_required()
    def get(self, request, parking_id, count_product_id):
        try:
            Parking.objects.get(parking_id=parking_id)
        except Parking.DoesNotExist:
            return HttpJsonResponse(status=404)
        try:
            product = ParkingCountProduct.objects.get(
                count_product_id=count_product_id)
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(status=404)
        code = 'count:%s' % product.count_product_id.hex
        count = get_sales_count(code)
        product.sales_count = count
        product.save()
        return HttpJsonResponse({'space_count': count})


class ParkingUserServicesDueTimeView(View):
    @session_required()
    def get(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_userservices:view',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingUserServicesQueryDueTimeForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        query, params = self._build_q(parking_id, data)
        services = UserService.objects.raw(query, params)[:1000]
        results = [_.detail_info() for _ in services]
        return HttpJsonResponse(results)

    def _nextpage_link(self, request, orders):
        return ''

    def _build_q(self, parking_id, data):
        data['product_ids'] = self._rel_product(parking_id, data)
        if data['product_type']:
            query = "select aaa.* from (select * from product_userservice where product_type in (%s) and service_status=1 and use_scope @> '{%s}') as aaa, (select car_ids, max(end_time) as end_time from product_userservice" % (
                ','.join(data['product_type']), '{%s}' % parking_id)
        else:
            query = "select aaa.* from (select * from product_userservice where service_status=1 and use_scope <@ '{%s}') as aaa, (select car_ids, max(end_time) as end_time from product_userservice" % parking_id
        query += ' where service_status=1 and use_scope @> %s'
        params = ['{%s}' % parking_id]
        if data['section'][0]:
            query += " and created_time > %s"
            params.append(data['section'][0])
        if data['section'][1]:
            query += " and created_time < %s"
            params.append(data['section'][1])
        if data['begin_section'][0]:
            query += " and end_time > %s"
            params.append(data['begin_section'][0])
        if data['begin_section'][1]:
            query += " and begin_time < %s"
            params.append(data['begin_section'][1])
        if data['end_section'][0]:
            query += " and end_time >= %s"
            params.append(data['end_section'][0])
        if data['end_section'][1]:
            query += " and end_time <= %s"
            params.append(data['end_section'][1])
        if data['region_id']:
            if data['product_ids']:
                query += " and product_id in (%s)" % ','.join(data['product_ids'])
            else:
                query += " and product_id in ('')"
        if data['product_type']:
            query += " and product_type in (%s)" % ','.join(data['product_type'])
            if data['product_type'] == ['2']:
                query += " and surplus_count > 0"
        if data['product_name']:
            query += " and product_name like %s"
            params.append('%%%s%%' % data['product_name'])
        if data['telephone']:
            query += "  and telephone like %s "
            params.append('%%%s%%' % data['telephone'])
        if data['car_id']:
            query += " and array_to_string(car_ids, ',') like %s"
            params.append('%%%s%%' % data['car_id'])
        if data['username']:
            query += " and username like %s"
            params.append('%%%s%%' % data['username'])
        current_time = now()
        if data['status'] == 0:
            pass
        elif data['status'] == 1:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time > %s"
            params.append(current_time)
        elif data['status'] == 2:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time <= %s"
            params.append(current_time)
        elif data['status'] == 3:
            query += " and service_status = 1"
            query += " and surplus_count = 0"
        elif data['status'] == 4:
            query += " and service_status = 2"
        query += ' group by car_ids having count(car_ids) >= 1) as ttt'
        query += ' where aaa.car_ids = ttt.car_ids'
        query += ' and aaa.end_time = ttt.end_time'
        if data['end_section'][0]:
            query += " and ttt.end_time > %s"
            params.append(data['end_section'][0])
        if data['end_section'][1]:
            query += " and ttt.end_time <= %s"
            params.append(data['end_section'][1])

        query += ' order by end_time asc, begin_time asc; '
        query = query[:-1]
        return query, params

    def _rel_product(self, parking_id, data):
        product_ids, p_types = [], data['product_type']
        if not data['region_id']:
            return product_ids
        stop_areas = StopArea.objects.filter(
            parking_id=parking_id,
            region_id=data['region_id'],
            is_deleted=False
        )
        for stop_area in stop_areas:
            subtype = stop_area.driver_subtype.detail_info()
            code = subtype['code'].split(':')
            if code[0] == 'timely' and ('1' in p_types or '4' in p_types):
                product_id = code[1] if code[1] != 'super' else '0'
            elif code[0] == 'count' and ('2' in p_types):
                product_id = code[1]
            elif code[0] == 'company_count' and ('3' in p_types):
                product_id = code[1]
            else:
                continue
            if product_id not in product_ids:
                product_ids.append(product_id)
        product_ids = [str('\'' + _ + '\'') for _ in product_ids]
        return product_ids


class CompanyUserServicesDueTimeView(View):
    @session_required()
    def get(self, request, company_id):
        if not check_perm(
                request, 'company_products_userservices:view',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyUserServicesQueryDueTimeForm, request.GET)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        parkings = Parking.objects.filter(company_id=company_id)
        parking_ids = [_.parking_id for _ in parkings]
        parking_ids = ','.join(parking_ids)
        query, params = self._build_q(parking_ids, data)
        services = UserService.objects.raw(query, params)[:1000]
        results = [_.detail_info() for _ in services]
        return HttpJsonResponse(results)

    def _nextpage_link(self, request, orders):
        return ''

    def _build_q(self, parking_ids, data):
        product_ids = None

        if data['product_type']:
            if data['parking_id']:
                query = "select aaa.* from (select * from product_userservice where product_type in (%s) and service_status=1 and use_scope @> '{%s}') as aaa, (select car_ids, max(end_time) as end_time from product_userservice" % (
                    ','.join(data['product_type']), '{%s}' % data['parking_id'])
            else:
                query = "select aaa.* from (select * from product_userservice where product_type in (%s) and service_status=1 and use_scope <@ '{%s}') as aaa, (select car_ids, max(end_time) as end_time from product_userservice" % (
                    ','.join(data['product_type']), parking_ids)
        else:
            query = "select aaa.* from (select * from product_userservice where 1=1 and service_status=1 and use_scope <@ '{%s}') as aaa, (select car_ids, max(end_time) as end_time from product_userservice" % parking_ids

        if data['parking_id']:
            product_ids = self._rel_product(data['parking_id'], data)
            query += ' where service_status=1 and use_scope @> %s'
            params = ['{%s}' % data['parking_id']]
        else:
            query += " where 1=1 and service_status=1 and use_scope <@ '{%s}'" % parking_ids
            params = []

        if data['section'][0]:
            query += " and created_time > %s"
            params.append(data['section'][0])
        if data['section'][1]:
            query += " and created_time < %s"
            params.append(data['section'][1])
        if data['begin_section'][0]:
            query += " and end_time > %s"
            params.append(data['begin_section'][0])
        if data['begin_section'][1]:
            query += " and begin_time < %s"
            params.append(data['begin_section'][1])
        if data['region_id']:
            if product_ids:
                query += " and product_id in (%s)" % ','.join(product_ids)
            else:
                query += " and product_id in ('')"
        if data['product_type']:
            query += " and product_type in (%s)" % ','.join(data['product_type'])
            if data['product_type'] == ['2']:
                query += " and surplus_count > 0"
        if data['product_name']:
            query += " and product_name like %s"
            params.append('%%%s%%' % data['product_name'])
        if data['telephone']:
            query += "  and telephone like %s "
            params.append('%%%s%%' % data['telephone'])
        if data['car_id']:
            query += " and array_to_string(car_ids, ',') like %s"
            params.append('%%%s%%' % data['car_id'])
        if data['username']:
            query += " and username like %s"
            params.append('%%%s%%' % data['username'])

        current_time = now()
        if data['status'] == 0:
            pass
        elif data['status'] == 1:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time > %s"
            params.append(current_time)
        elif data['status'] == 2:
            query += " and service_status = 1"
            query += " and surplus_count > 0"
            query += " and end_time <= %s"
            params.append(current_time)
        elif data['status'] == 3:
            query += " and service_status = 1"
            query += " and surplus_count = 0"
        elif data['status'] == 4:
            query += " and service_status = 2"

        query += ' group by car_ids having count(car_ids) >= 1) as ttt'
        query += ' where aaa.car_ids = ttt.car_ids'
        query += ' and aaa.end_time = ttt.end_time'
        if data['end_section'][0]:
            query += " and ttt.end_time >= %s"
            params.append(data['end_section'][0])
        if data['end_section'][1]:
            query += " and ttt.end_time <= %s"
            params.append(data['end_section'][1])

        query += ' order by end_time asc, begin_time asc; '
        query = query[:-1]
        return query, params

    def _rel_product(self, parking_id, data):
        product_ids, p_types = [], data['product_type']
        if not data['region_id']:
            return product_ids
        stop_areas = StopArea.objects.filter(
            parking_id=parking_id,
            region_id=data['region_id'],
            is_deleted=False
        )
        for stop_area in stop_areas:
            subtype = stop_area.driver_subtype.detail_info()
            code = subtype['code'].split(':')
            if code[0] == 'timely' and ('1' in p_types or '4' in p_types):
                product_id = code[1] if code[1] != 'super' else '0'
            elif code[0] == 'count' and ('2' in p_types):
                product_id = code[1]
            elif code[0] == 'company_count' and ('3' in p_types):
                product_id = code[1]
            else:
                continue
            if product_id not in product_ids:
                product_ids.append(product_id)
        product_ids = [str('\'' + _ + '\'') for _ in product_ids]
        return product_ids



class ParkingProductOrdersBatchView(View):

    @session_required()
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_orders:add',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingProductOrderBatchCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        returndata = []
        if data['service_ids']:
            for service_id in data['service_ids']:
                try:
                    params = {
                        'use_scope__contains': [parking_id],
                        'service_id': service_id,
                    }
                    service = UserService.objects.get(**params)
                except UserService.DoesNotExist:
                    resource = 'parking_timely_product'
                    if  data['product_type'] == 2:
                        resource = 'parking_count_product'
                    return  HttpJsonResponse(errors_422([
                                {'resource': resource, 'code': 'not_found'}
                            ]), status=422)
                service_info = service.detail_info()

                data_for={
                    "product_type" : data.get("product_type"),
                    "product_id" : data.get("product_id"),
                    "telephone" : service_info.get("telephone"),
                    "car_ids" : service_info.get("car_ids"),
                    "start_date" : data.get("start_date"),
                    "username" : service_info.get("username"),
                    "address" : service_info.get("address"),
                    "comments" : service_info.get("comments"),
                    "paid_amount" : data.get("paid_amount"),
                    "company_id" :  ParkingHelper.get(parking_id).company_id,
                    "where_bought" : parking_id
                }

                if data_for['product_type'] == 1:
                    datas = self._buy_timely_product(request, data_for)
                elif data_for['product_type'] == 2:
                    datas = self._buy_count_product(request, data_for)
                else:
                    return HttpJsonResponse(errors_422([
                        {'field': 'product_type', 'code': 'invalid'}
                    ]), status=422)
                if not isinstance(datas,dict):
                    return datas
                returndata.append(datas)
            return HttpJsonResponse({"data":returndata}, status=201)
        else:
            return HttpJsonResponse(errors_422(data), status=422)



    def _build_order(self, params, product):
        #         data = {"mobile": params['telephone']}
        #         user_id = get_htcode(data)
        order = {
            #             'user_id': user_id,
            'product_id': params['product_id'],
            'product_type': params['product_type'],
            'begin_time': params['start_date'],
            'car_ids': params['car_ids'],
            'use_scope': [product.parking_id],
            'parallel_num': product.parallel_num,
            # 'price': product.price,
            # 'paid_amount': params['paid_amount'],
            'paid_way': 'CASH',
            'paid_time': now(),
            'order_status': 2,
            'telephone': params['telephone'],
            'company_id': params['company_id'],
            'where_bought': params['where_bought'],
        }
        if product.duration_unit == 1:
            duration = timedelta(days=product.duration)
            order['end_time'] = params['start_date'] + duration
        else:
            end_time = addmonths(params['start_date'], product.duration)
            if end_time.day < params['start_date'].day:
                end_time += timedelta(days=1)
                end_time = end_time.replace(hour=0, minute=0, second=0)
            order['end_time'] = end_time
        if params['product_type'] == 1 and product.start_cycle_unit in [2, 3]:
            price, static_time = product.static_time_with_part_prices(
                params['start_date'], order['end_time'])
            order['paid_amount'] = params['paid_amount']
            order['price'] = price
            order['end_time'] = static_time
        else:
            order['paid_amount'] = params['paid_amount']
            order['price'] = product.price
        if params['product_type'] == 1:
            order['product_name'] = product.timely_product_name
        else:
            order['product_name'] = product.count_product_name
        return order

    def _build_service(self, product, order, params):
        #         data = {"mobile": order['telephone']}
        #         user_id = get_htcode(data)
        service = {
            #             'user_id': user_id,
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'service_detail': product.detail_info(),
            'car_type': product.car_type,
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'telephone': order['telephone'],
            'company_id': order['company_id'],
            'where_bought': order['where_bought'],
            'username': params['username'],
            'address': params['address'],
            'comments': params['comments']

        }
        if order['product_type'] == 1:
            service['surplus_count'] = 9999
        else:
            service['surplus_count'] = product.count

        return service


    @method_decorator(atomic)
    def _buy_timely_product(self, request, params):
        try:
            product = ParkingTimelyProduct.objects.get(
                timely_product_id=params['product_id'])
        except ParkingTimelyProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_timely_product', 'code': 'not_found'}
            ]), status=422)
        spot = parking_spot('timely:%s' % product.timely_product_id.hex)
        if product.deleted:
            return HttpJsonResponse(status=410)
        if not product.enabled:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_timely_product', 'code': 'not_allow'}
            ]), status=422)
        if 2 not in product.purchase_mode:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_timely_product', 'code': 'not_allow'}
            ]), status=422)
        order = self._build_order(params, product)
        service = self._build_service(product, order, params)
        order = Order.objects.create(**order)
        service['order_id'] = order.order_id.hex
        service['spots'] = spot
        service['used_quota_detail'] = {
            'quota_type': product.quota_type,
            'quota': product.quota,
            'quota_of_once': product.quota_of_once,
            'used_quota': {}
        }
        service = UserService.objects.create(**service)
        product_message(order)
        send_user_service_to_parking(service, 'create')
        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.use_scope[0] if order.product_type != 3 else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'service', 'income', '产品购买',
            parking.company_id, order.paid_way, order.paid_time,
            parking_id, operator,
            order.detail_info(), order.paid_amount)
        # 记日志
        log_parking_product_order(
            request,
            parking,
            order,
            'buy',
            'timely',
            params['username']
        )
        return {
            'order_id': order.order_id.hex,
            'created_time': dtt(order.created_time),
            'service_id': service.service_id.hex,
            'spots': spot
        }

    @method_decorator(atomic)
    def _buy_count_product(self, request, params):
        try:
            product = ParkingCountProduct.objects.get(
                count_product_id=params['product_id'])
        except ParkingCountProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_count_product', 'code': 'not_found'}
            ]), status=422)
        spot = parking_spot('count:%s' % product.count_product_id.hex)
        if product.deleted:
            return HttpJsonResponse(status=410)
        if not product.enabled:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_count_product', 'code': 'not_allow'}
            ]), status=422)
        if 2 not in product.purchase_mode:
            return HttpJsonResponse(errors_422([
                {'resource': 'parking_count_product', 'code': 'not_allow'}
            ]), status=422)
        order = self._build_order(params, product)
        service = self._build_service(product, order, params)
        order = Order.objects.create(**order)
        service['order_id'] = order.order_id.hex
        service['spots'] = spot
        service = UserService.objects.create(**service)
        product_message(order)
        send_user_service_to_parking(service, 'create')
        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.use_scope[0] if order.product_type != 3 else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'service', 'income', '产品购买',
            parking.company_id, order.paid_way, order.paid_time,
            parking_id, operator,
            order.detail_info(), order.paid_amount)
        # 记日志
        log_parking_product_order(request,
                                  parking,
                                  order,
                                  'buy',
                                  'count',
                                  params['username'])
        return {
            'order_id': order.order_id.hex,
            'created_time': dtt(order.created_time),
            'service_id': service.service_id.hex,
            'spots': spot
        }


class CompanyProductOrdersBatchView(View):
    @session_required()
    def post(self, request, company_id):
        if not check_perm(
                request, 'company_products_orders:add',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            CompanyProductOrderBatchCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        returndata = []
        if data['service_ids']:
            for service_id in data['service_ids']:
                try:
                    params = {
                        'service_id': service_id
                    }
                    service = UserService.objects.get(**params)
                except UserService.DoesNotExist:
                    return HttpJsonResponse(errors_422([
                        {'resource': 'company_count_product', 'code': 'not_found'}
                    ]), status=422)
                service_info = service.detail_info()
                data_for = {
                    "product_type": data.get("product_type"),
                    "product_id":  data.get("product_id"),
                    "telephone": service_info.get("telephone"),
                    "car_ids": service_info.get("car_ids"),
                    "start_date": data.get("start_date"),
                    "username": service_info.get("username"),
                    "address": service_info.get("address"),
                    "comments": service_info.get("comments"),
                    "paid_amount": data.get("paid_amount"),
                    "company_id": company_id,
                    "where_bought": data.get("where_bought")
                }
                data_for['parking_id'] =""
                if data_for['where_bought']:
                    data_for['parking_id'] = data_for['where_bought']

                if data_for['product_type'] != 3:
                    return HttpJsonResponse(errors_422([
                        {'field': 'product_type', 'code': 'invalid'}
                    ]), status=422)
                else:
                    datas = self._buy_company_product(request, company_id, data_for)

                if not isinstance(datas, dict):
                    return datas
                returndata.append(datas)
            return HttpJsonResponse({"data": returndata}, status=201)
        else:
            return HttpJsonResponse(errors_422(data), status=422)


    def _build_order(self, params, product):
        #         data = {"mobile": params['telephone']}
        #         user_id = get_htcode(data)
        order = {
            #             'user_id': user_id,
            'product_id': params['product_id'],
            'product_name': product.count_product_name,
            'product_type': params['product_type'],
            'begin_time': params['start_date'],
            'car_ids': params['car_ids'],
            'use_scope': product.use_scope,
            'parallel_num': product.parallel_num,
            'price': product.price,
            'paid_amount': params['paid_amount'],
            'paid_way': 'CASH',
            'paid_time': now(),
            'order_status': 2,
            'telephone': params['telephone'],
            'company_id': params['company_id'],
            'where_bought': params['where_bought']
        }

        if product.duration_unit == 1:
            duration = timedelta(days=product.duration)
            order['end_time'] = params['start_date'] + duration
        else:
            end_time = addmonths(params['start_date'], product.duration)
            if end_time.day < params['start_date'].day:
                end_time += timedelta(days=1)
                end_time.replace(hour=0, minute=0, second=0)
            order['end_time'] = end_time

        return order

    def _build_service(self, product, order, params):
        #         data = {"mobile": order['telephone']}
        #         user_id = get_htcode(data)
        service = {
            #             'user_id': user_id,
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'service_detail': product.detail_info(),
            'surplus_count': product.count,
            'car_type': product.car_type,
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'telephone': order['telephone'],
            'company_id': order['company_id'],
            'where_bought': params['where_bought'],
            'username': params['username'],
            'address': params['address'],
            'comments': params['comments']

        }

        return service

    @method_decorator(atomic)
    def _buy_company_product(self, request, company_id, params):
        try:
            product = CompanyCountProduct.objects.get(
                count_product_id=params['product_id'])
        except CompanyCountProduct.DoesNotExist:
            return HttpJsonResponse(errors_422([
                {'resource': 'company_count_product', 'code': 'not_found'}
            ]), status=422)
        spot = parking_spot('company_count:%s' % product.count_product_id.hex)
        if product.deleted:
            return HttpJsonResponse(status=410)
        if not product.enabled:
            return HttpJsonResponse(errors_422([
                {'resource': 'company_count_product', 'code': 'not_allow'}
            ]), status=422)
        if 2 not in product.purchase_mode:
            return HttpJsonResponse(errors_422([
                {'resource': 'company_count_product', 'code': 'not_allow'}
            ]), status=422)
        order = self._build_order(params, product)
        service = self._build_service(product, order, params)
        order = Order.objects.create(**order)
        service['order_id'] = order.order_id.hex
        service['spots'] = spot
        service = UserService.objects.create(**service)
        # 发送购买短信
        product_message(order)
        send_user_service_to_parking(service, 'create')
        parking = ParkingHelper().get(order.use_scope[0])
        parking_id = order.where_bought if order.where_bought else None
        operator = get_operator(request, parking.company_id)
        BillDetailUtils.bill_detail(
            'service', 'income', '产品购买',
            parking.company_id, order.paid_way, order.paid_time,
            parking_id, operator,
            order.detail_info(), order.paid_amount)

        # 记日志
        log_company_product_order(request, company_id, order, 'buy', params['username'])
        return {
            'order_id': order.order_id.hex,
            'created_time': dtt(order.created_time),
            'service_id': service.service_id.hex,
            'spots': spot
        }


class ParkingSuperProductOrdersBatchView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_orders:add',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingSuperProductOrderBatchCreateForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)
        returndata = []
        if data['service_ids']:
            for service_id in data['service_ids']:
                try:
                    params = {
                        'service_id': service_id
                    }
                    service = UserService.objects.get(**params)
                except UserService.DoesNotExist:
                    return HttpJsonResponse(errors_422([
                        {'resource': 'company_count_product', 'code': 'not_found'}
                    ]), status=422)
                parking=ParkingHelper().get(parking_id)
                service_info = service.detail_info()
                data["parking_id"] = parking_id
                data["telephone"] = service_info.get("telephone")
                data["car_ids"] = service_info.get("car_ids")
                data["username"] = service_info.get("username")
                data["address"] = service_info.get("address")
                data["comments"] = service_info.get("comments")

                product = self._build_product(data)
                order = self._build_order(data, product)
                order['where_bought'] = parking_id
                order['company_id'] = parking.company_id
                service = self._build_service(data, order)
                order = Order.objects.create(**order)
                # 购买超级包时产品短信提醒
                product_message(order)

                service['order_id'] = order.order_id.hex
                service['where_bought'] = parking_id
                service['company_id'] = parking.company_id
                service = UserService.objects.create(**service)
                send_user_service_to_parking(service, 'create')
                operator = get_operator(request, parking.company_id)
                BillDetailUtils.bill_detail(
                    'service', 'income', '产品购买',
                    parking.company_id, order.paid_way, order.paid_time,
                    parking_id, operator,
                    order.detail_info(), order.paid_amount)
                log_parking_product_order(
                    request, parking, order, 'buy', 'super_timely')

                datas={
                    'order_id': order.order_id.hex,
                    'created_time': dtt(order.created_time),
                    'service_id': service.service_id.hex,
                }
                returndata.append(datas)
            return HttpJsonResponse({"data": returndata}, status=201)
        else:
            return HttpJsonResponse(errors_422(data), status=422)

    def _build_product(self, data):
        product = {
            'product_id': '0',
            'product_name': data['product_name'],
            'parking_id': data['parking_id'],
            'sales_count': 1,
            'car_type': data['car_type'],
            'start_date': data['start_date'],
            'end_date': data['end_date'],
            'price': data['paid_amount'],
            'refund_mode': data['refund_mode'],
            'service_durations': data['service_durations'],
            'parallel_num': data['parallel_num'],
            'comments': data['comments'],
            'quota_type': data['quota_type'],
            'quota': data['quota'],
            'quota_of_once': data['quota_of_once']
        }
        return product

    def _build_order(self, params, product):
        order = {
            'product_id': product['product_id'],
            'product_name': params['product_name'],
            'product_type': params['product_type'],
            'begin_time': params['start_date'],
            'end_time': params['end_date'],
            'car_ids': params['car_ids'],
            'use_scope': [product['parking_id']],
            'parallel_num': product['parallel_num'],
            'price': product['price'],
            'paid_amount': params['paid_amount'],
            'paid_way': 'CASH',
            'paid_time': now(),
            'order_status': 2,
            'telephone': params['telephone'],
        }
        return order

    def _build_service(self, product, order):
        product2 = copy.deepcopy(product)
        product2['start_date'] = dtt(product['start_date'])
        product2['end_date'] = dtt(product['end_date'])
        service = {
            'product_id': order['product_id'],
            'product_name': order['product_name'],
            'product_type': order['product_type'],
            'service_detail': product2,
            'car_type': product['car_type'],
            'begin_time': order['begin_time'],
            'end_time': order['end_time'],
            'car_ids': order['car_ids'],
            'use_scope': order['use_scope'],
            'parallel_num': order['parallel_num'],
            'service_status': 1,
            'telephone': order['telephone'],
            'surplus_count': 9999,
            'username': product['username'],
            'address': product['address'],
            'comments': product['comments'],
            'used_quota_detail': {
                'quota_type': product['quota_type'],
                'quota': product['quota'],
                'quota_of_once': product['quota_of_once'],
                'used_quota': {}
            }
        }
        return service



class ParkingProductOrderBatchRefundView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, parking_id):
        if not check_perm(
                request, 'parking_products_orders:refund',
                parking_id=parking_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingProductOrderBatchRefundForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        returndata = []
        if data['product_order_ids']:
            for product_order_id in data['product_order_ids']:

                try:
                    params = {
                        'use_scope__contains': [parking_id],
                        'order_id': product_order_id,
                    }
                    order = Order.objects.get(**params)
                except Order.DoesNotExist:
                    return HttpJsonResponse(status=404)
                if order.order_status != 2:
                    continue
                    # return HttpJsonResponse(errors_422([
                    #     {'resource': 'order', 'code': 'not_allow'}
                    # ]), status=422)

                try:
                    service = UserService.objects.filter(**params).first()
                except UserService.DoesNotExist:
                    return HttpJsonResponse(errors_422([
                        {'resource': 'user_service', 'code': 'not_found'}
                    ]), status=422)

                if service.service_status != 1 or service.end_time < now():
                    continue
                    # return HttpJsonResponse(errors_422([
                    #     {'resource': 'user_service', 'code': 'not_allow'}
                    # ]), status=422)

                order.refund_amount = data['refund_amount']
                order.refund_way = 'CASH'
                order.refund_time = now()
                order.order_status = 4
                order.save()
                service.service_status = 2
                service.save()
                update_service_to_app(
                    service.app_id,
                    order.order_id, service.service_id,
                    order.refund_amount, order.refund_way,
                    order.refund_time, order.order_status, service.service_status)
                send_user_service_to_parking(service, 'delete')

                parking = ParkingHelper().get(order.use_scope[0])
                parking_id = order.use_scope[0] if order.product_type != 3 else None
                operator = get_operator(request, parking.company_id)
                BillDetailUtils.bill_detail(
                    'refund', 'expense', '产品退款',
                    parking.company_id, order.refund_way, order.refund_time,
                    parking_id, operator,
                    order.detail_info(), order.refund_amount)

                # 记日志
                #         try:
                #             parking = Parking.objects.get(parking_id=parking_id)
                #         except Parking.DoesNotExist:
                #             return HttpJsonResponse(status=404)
                log_parking_product_refund(request, parking, order, 'refund')
                datas ={
                    'order_id': product_order_id,
                    'service_id': service.service_id.hex,
                    'refund_time': dtt(order.refund_time),
                }
                returndata.append(datas)
            return HttpJsonResponse({"data": returndata}, status=201)
        else:
            return HttpJsonResponse(errors_422(data), status=422)



class CompanyProductOrderBatchRefundView(View):
    @session_required()
    @method_decorator(atomic)
    def post(self, request, company_id):
        if not check_perm(
                request, 'company_products_orders:refund',
                company_id=company_id):
            return HttpResponseForbidden()
        is_valid, data = validate_form(
            ParkingProductOrderBatchRefundForm, request.jsondata)
        if not is_valid:
            return HttpJsonResponse(errors_422(data), status=422)

        returndata = []
        if data['product_order_ids']:
            for product_order_id in data['product_order_ids']:

                try:
                    order = Order.objects.get(order_id=product_order_id)
                except Order.DoesNotExist:
                    return HttpJsonResponse(status=404)
                if order.order_status != 2:
                    continue
                    # return HttpJsonResponse(errors_422([
                    #     {'resource': 'order', 'code': 'not_allow'}
                    # ]), status=422)

                try:
                    service = UserService.objects.filter(
                        order_id=product_order_id).first()
                except UserService.DoesNotExist:
                    return HttpJsonResponse(errors_422([
                        {'resource': 'user_service', 'code': 'not_found'}
                    ]), status=422)

                if service.service_status != 1 or service.end_time < now():
                    continue
                    # return HttpJsonResponse(errors_422([
                    #     {'resource': 'user_service', 'code': 'not_allow'}
                    # ]), status=422)

                order.refund_amount = data['refund_amount']
                order.refund_way = 'CASH'
                order.refund_time = now()
                order.order_status = 4
                order.save()
                service.service_status = 2
                service.save()
                update_service_to_app(
                    service.app_id,
                    order.order_id, service.service_id,
                    order.refund_amount, order.refund_way,
                    order.refund_time, order.order_status, service.service_status)
                send_user_service_to_parking(service, 'delete')

                parking = ParkingHelper().get(order.use_scope[0])
                parking_id = order.where_bought if order.where_bought else None
                operator = get_operator(request, parking.company_id)
                BillDetailUtils.bill_detail(
                    'refund', 'expense', '产品退款',
                    parking.company_id, order.refund_way, order.refund_time,
                    parking_id, operator,
                    order.detail_info(), order.refund_amount)
                # 记日志
                log_company_product_refund(request, company_id, order, 'refund')
                datas = {
                    'order_id': product_order_id,
                    'service_id': service.service_id.hex,
                    'refund_time': dtt(order.refund_time),
                }
                returndata.append(datas)
            return HttpJsonResponse({"data": returndata}, status=201)
        else:
            return HttpJsonResponse(errors_422(data), status=422)
