import redis_read_and_write
from db_operation import MySqlHelper
import json
from datetime import datetime
import requests


class ApiRequest(object):
    def __init__(self):
        requests.DEFAULT_RETRIES = 5
        self.res = requests.session()
        self.res.keep_alive = False

    def request(self, url, data):
        for i in range(0, 3):
            try:
                result = self.res.post(url, data=data).json()
                return result
            except Exception as e:
                if i == 2:
                    return f"request_failed:{str(e)}"
                else:
                    pass


class TaskDataOperate(object):
    def __init__(self, wcs_task_number, log_name, db=MySqlHelper(), res=ApiRequest()):
        self.db = db
        self.wcs_task_number = wcs_task_number
        self.log_name = log_name
        self.res = res

    def write_wms_goods_box_db(self, box_rfid, box_status, box_location, locate_list=None, qr_code=None,
                               material_receipt_id=None, plan_id=None, box_list_id=None, material_id=None,
                               material_name=None, material_type=None, goods_type_code=None, batch_num=None,
                               erp_mo_num_row_num=None, erp_mo_num=None, goods_qty=0, process_num=None,
                               process_name=None, next_process_num=None, next_process_name=None, shouliao_weight=None,
                               actual_weight=None, order_num=None, next_material_id=None, next_material_name=None,
                               next_erp_mo_num=None, flakiness=None, slb_num=None, location_name=None, location_id=None,
                               validity_time=None, production_time=None, user_id=None, lock_num="0", create_time=None,
                               wcs_task_number=None):
        update_data_sql = "update wms_goods_box set box_status = %s, box_location = %s, locate_list = %s, " \
                          "qr_code = %s, material_receipt_id = %s, plan_id = %s, box_list_id = %s, material_id = %s, " \
                          "material_name = %s, material_type = %s, goods_type_code = %s, batch_num = %s, " \
                          "erp_mo_num_row_num = %s, erp_mo_num = %s, goods_qty = %s, process_num = %s, " \
                          "process_name = %s, next_process_num = %s, next_process_name = %s, shouliao_weight = %s, " \
                          "actual_weight = %s, order_num = %s, next_material_id = %s, next_material_name = %s, " \
                          "next_erp_mo_num = %s, flakiness = %s, slb_num = %s, location_name = %s, location_id = %s, " \
                          "validity_time = %s, production_time = %s, user_id = %s, lock_num = %s, create_time = %s, " \
                          "wcs_task_number = %s where box_rfid = %s"
        update_param_list = [box_status, box_location, locate_list, qr_code, material_receipt_id, plan_id,
                             box_list_id, material_id, material_name, material_type, goods_type_code, batch_num,
                             erp_mo_num_row_num, erp_mo_num, goods_qty, process_num, process_name, next_process_num,
                             next_process_name, shouliao_weight, actual_weight, order_num, next_material_id,
                             next_material_name, next_erp_mo_num, flakiness, slb_num, location_name, location_id,
                             validity_time, production_time, user_id, lock_num, create_time, wcs_task_number, box_rfid]
        self.db.insert_or_update_db_data(update_data_sql, update_param_list)

    def out_lk_srm_task_end(self):
        try:
            get_data_sql = f"select id, start_site, box_rfid_one, box_rfid_two, box_rfid_three, box_rfid_four " \
                           f"from wms_task where wcs_task_number = '{self.wcs_task_number}' order by id desc limit 1"
            all_data = self.db.get_db_data(get_data_sql)
            if all_data != ():
                box_rfid_list = []
                task_id = all_data[0][0]
                location_name = all_data[0][1]
                box_rfid_one = all_data[0][2]
                if box_rfid_one != "no_one" and box_rfid_one is not None:
                    box_rfid_list.append(box_rfid_one)
                else:
                    pass
                box_rfid_two = all_data[0][3]
                if box_rfid_two != "no_one" and box_rfid_two is not None:
                    box_rfid_list.append(box_rfid_two)
                else:
                    pass
                box_rfid_three = all_data[0][4]
                if box_rfid_three != "no_one" and box_rfid_three is not None:
                    box_rfid_list.append(box_rfid_three)
                else:
                    pass
                box_rfid_four = all_data[0][5]
                if box_rfid_four != "no_one" and box_rfid_four is not None:
                    box_rfid_list.append(box_rfid_four)
                else:
                    pass
                # 改写所有货盒在库库位（location_name）为空
                if box_rfid_list:
                    for box in box_rfid_list:
                        update_state_sql = f"update wms_goods_box set location_name = %s where box_rfid = %s"
                        update_param_list = [None, box]
                        self.db.insert_or_update_db_data(update_state_sql, update_param_list)
                else:
                    log = f"获取任务中货盒列表失败"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # 改写设备任务结束时间
                try:
                    update_param_sql = "update wms_task set srm_task_end_time = now(3) where id = %s"
                    update_param_list = [task_id]
                    self.db.insert_or_update_db_data(update_param_sql, update_param_list)
                except Exception as e:
                    log = f"堆垛机or多穿出库结束改写wms_task报错{str(e)}"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # 将出库库位状态改为empty，清空货位RFID
                try:
                    update_data_sql = "update lk_location set location_state = %s, box_type = %s, " \
                                      "layer_one_box_rfid = %s, layer_two_box_rfid = %s, layer_three_box_rfid = %s, " \
                                      "layer_four_box_rfid = %s where lk_location_name = %s"
                    if location_name[0:2] in ["15", "16"]:
                        update_param_list = ["lock", None, "no_one", "no_one", "no_one", "no_one", location_name]
                    else:
                        update_param_list = ["empty", None, "no_one", "no_one", "no_one", "no_one", location_name]
                    self.db.insert_or_update_db_data(update_data_sql, update_param_list)
                except Exception as e:
                    log = f"堆垛机or多穿出库结束改写lk_location报错{str(e)}"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # redis输出日志
                log = f"堆垛机出库任务完成，改写堆垛机工作状态及数据库任务相关表格改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            else:
                log = f"堆垛机入库结束查询任务信息失败"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
        except Exception as e:
            log = f"堆垛机出库结束获取任务参数报错{str(e)}"
            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)

    def out_lk_line_task_begin(self):
        try:
            get_data_sql = f"select id, task_track from wms_task where wcs_task_number = '{self.wcs_task_number}' " \
                           f"order by id desc limit 1"
            data = self.db.get_db_data(get_data_sql)
            if data != ():
                task_id = data[0][0]
                task_track_json = data[0][1]
                task_track_list = json.loads(task_track_json)
                task_track_list[1]["status"] = "1"
                task_track_json_new = json.dumps(task_track_list)
                try:
                    update_data_sql = f"update wms_task set line_task_begin_time = now(3), task_state = %s, " \
                                      f"task_track = %s where id = %s"
                    update_param_list = ["on_line", task_track_json_new, task_id]
                    self.db.insert_or_update_db_data(update_data_sql, update_param_list)
                except Exception as e:
                    log = f"出库任务上线改写wms_task报错{str(e)}"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            else:
                log = f"出库任务上线查询任务信息失败"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
        except Exception as e:
            log = f"出库任务上线查询任务信息报错{str(e)}"
            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)

    def out_lk_task_end(self):
        try:
            # 用于出库任务，货盒到达传输线终点时所有数据的处理
            # 1、改写wms_task的task_end、line_task_end_time，task_track
            # 2、改写wms_goods_box为free、empty，其余全空

            # 按照任务号查询任务数据
            get_data_sql = f"select id, task_track, box_rfid_one, box_rfid_two, box_rfid_three, box_rfid_four, " \
                           f"box_type from wms_task where wcs_task_number = '{self.wcs_task_number}' " \
                           f"order by id desc limit 1"
            data = self.db.get_db_data(get_data_sql)
            if data != ():
                task_id = data[0][0]
                task_track_json = data[0][1]  # 获取任务轨迹列表的json格式
                box_rfid_one = data[0][2]
                box_rfid_list = [box_rfid_one]  # 拼接任务中货盒列表
                box_rfid_two = data[0][3]
                if box_rfid_two != "no_one" and box_rfid_two is not None:
                    box_rfid_list.append(box_rfid_two)  # 拼接任务中货盒列表
                box_rfid_three = data[0][4]
                if box_rfid_three != "no_one" and box_rfid_three is not None:
                    box_rfid_list.append(box_rfid_three)  # 拼接任务中货盒列表
                box_rfid_four = data[0][5]
                if box_rfid_four != "no_one" and box_rfid_four is not None:
                    box_rfid_list.append(box_rfid_four)  # 拼接任务中货盒列表
                box_type = data[0][6]
                task_track_list = json.loads(task_track_json)  # 提取任务轨迹的列表
                task_track_list[2]["status"] = "1"
                task_track_json_new = json.dumps(task_track_list)  # 改写任务轨迹列表后转回json格式
                # 改写wms_task表的line_task_end_time、task_end、task_track
                update_data_sql = f"update wms_task set line_task_end_time = now(3), task_state = %s, " \
                                  f"task_track = %s where id = %s"
                update_param_list = ["task_end", task_track_json_new, task_id]
                self.db.insert_or_update_db_data(update_data_sql, update_param_list)
                log = f"出库任务终点wms_task数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # 将所有货盒数据改空
                for box in box_rfid_list:
                    self.write_wms_goods_box_db(box_rfid=box, box_status="empty", box_location="free")
                    if int(box_type) == 41:
                        get_data_sql = f"select qr_code from wms_goods_in_box where out_lk_task_id = {task_id}"
                        get_data = self.db.get_db_data(get_data_sql)
                        if get_data == () or get_data[0][0] is None:
                            log = f"{self.wcs_task_number}查询任务物料信息失败"
                            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                        else:
                            qr_code = get_data[0][0]
                            update_data_sql = f"update wms_mujv_location set location_name = %s, " \
                                              f"action_time = now(3) where qr_code = %s"
                            update_param_list = ["free", qr_code]
                            self.db.insert_or_update_db_data(update_data_sql, update_param_list)
                    else:
                        pass
                log = f"出库任务终点'{box_rfid_list}'wms_goods_box数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            else:
                log = f"传输线任务终点数据处理获取任务数据失败"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            box_rfid = redis_read_and_write.redis_hget(f"line_task:{self.wcs_task_number}", "box_rfid")
            redis_read_and_write.redis_hdel('rfid_for_task_number', box_rfid)
        except Exception as e:
            log = f"传输线任务结束改写相关数据报错{str(e)}"
            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)

    def reya_line_task_end(self, end_site):
        try:
            # 用于去热压的出库任务到达传输线终点，货盒到达传输线终点时所有数据的处理
            # 1、改写wms_task的task_end、line_task_end_time，task_track
            # 2、如果传输线的终点即为任务终点改写wms_goods_box为free、empty，其余全空
            # 3、如果传输线终点不是任务终点，不改写wms_goods_box

            # 按照任务号查询任务数据
            get_data_sql = f"select id, task_track, box_rfid_one, box_rfid_two, box_rfid_three, box_rfid_four, " \
                           f"destination_site from wms_task where wcs_task_number = '{self.wcs_task_number}' " \
                           f"order by id desc limit 1"
            data = self.db.get_db_data(get_data_sql)
            if data != ():
                task_id = data[0][0]
                task_track_json = data[0][1]  # 获取任务轨迹列表的json格式
                box_rfid_one = data[0][2]
                box_rfid_list = [box_rfid_one]  # 拼接任务中货盒列表
                box_rfid_two = data[0][3]
                if box_rfid_two != "no_one" and box_rfid_two is not None:
                    box_rfid_list.append(box_rfid_two)  # 拼接任务中货盒列表
                box_rfid_three = data[0][4]
                if box_rfid_three != "no_one" and box_rfid_three is not None:
                    box_rfid_list.append(box_rfid_three)  # 拼接任务中货盒列表
                box_rfid_four = data[0][5]
                if box_rfid_four != "no_one" and box_rfid_four is not None:
                    box_rfid_list.append(box_rfid_four)  # 拼接任务中货盒列表
                destination_site = data[0][6]
                task_track_list = json.loads(task_track_json)  # 提取任务轨迹的列表
                task_track_list[2]["status"] = "1"
                task_track_json_new = json.dumps(task_track_list)  # 改写任务轨迹列表后转回json格式
                # 如果当前站点为任务终点
                if end_site == destination_site:
                    task_end_log = "task_end"
                    # 将所有货盒数据改空
                    for box in box_rfid_list:
                        self.write_wms_goods_box_db(box_rfid=box, box_status="empty", box_location="free")
                    log = f"出库任务终点'{box_rfid_list}'wms_goods_box数据改写完成"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                else:
                    task_end_log = "line_task_end"
                # 改写wms_task表的line_task_end_time、task_end、task_track
                update_data_sql = f"update wms_task set line_task_end_time = now(3), task_state = %s, " \
                                  f"task_track = %s where id = %s"
                update_param_list = [task_end_log, task_track_json_new, task_id]
                self.db.insert_or_update_db_data(update_data_sql, update_param_list)
                log = f"出库任务终点wms_task数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            else:
                log = f"传输线任务终点数据处理获取任务数据失败"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            box_rfid = redis_read_and_write.redis_hget(f"line_task:{self.wcs_task_number}", "box_rfid")
            redis_read_and_write.redis_hdel('rfid_for_task_number', box_rfid)
        except Exception as e:
            log = f"热压出库任务结束改写相关数据报错{str(e)}"
            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)

    def into_lk_line_task_end(self):
        try:
            get_data_sql = f"select id, task_track from wms_task where wcs_task_number = {self.wcs_task_number} " \
                           f"order by id desc limit 1"
            data = self.db.get_db_data(get_data_sql)
            if data != ():
                task_id = data[0][0]
                task_track_json = data[0][1]
                task_track_list = json.loads(task_track_json)
                task_track_list[1]["status"] = "1"
                task_track_json_new = json.dumps(task_track_list)
                update_data_sql = "update wms_task set line_task_end_time = now(3), task_track = %s where id = %s"
                update_data_list = [task_track_json_new, task_id]
                self.db.insert_or_update_db_data(update_data_sql, update_data_list)
                log = f"入库任务传输线终点数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            else:
                log = f"入库任务传输线终点数据处理获取任务数据失败"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            box_rfid = redis_read_and_write.redis_hget(f"line_task:{self.wcs_task_number}", "box_rfid")
            redis_read_and_write.redis_hdel('rfid_for_task_number', box_rfid)
        except Exception as e:
            log = f"入库任务传输线终点数据改写相关数据报错{str(e)}"
            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)

    def into_lk_task_end(self, handing_way="auto"):
        try:
            # 用于入库任务，货盒在入库设备任务完成时所有数据的处理
            # 1、改写wms_task的task_end、line_task_end_time，task_track
            # 2、改写wms_goods_box为free、empty，其余全空
            # 23、改写lk_location为full，所有box_rfid，box_type

            # 按照任务号查询任务数据
            get_data_sql = f"select id, destination_site, box_rfid_one, box_rfid_two, box_rfid_three, box_rfid_four, " \
                           f"box_type, task_track, user_id from wms_task " \
                           f"where wcs_task_number = '{self.wcs_task_number}' order by id desc limit 1"
            all_data = self.db.get_db_data(get_data_sql)
            if all_data != ():
                box_rfid_list = []
                task_id = all_data[0][0]
                location_name = all_data[0][1]
                box_rfid_one = all_data[0][2]
                if box_rfid_one != "no_one" and box_rfid_one is not None:
                    box_rfid_list.append(box_rfid_one)  # 拼接任务中货盒列表
                else:
                    pass
                box_rfid_two = all_data[0][3]
                if box_rfid_two != "no_one" and box_rfid_two is not None:
                    box_rfid_list.append(box_rfid_two)  # 拼接任务中货盒列表
                else:
                    pass
                box_rfid_three = all_data[0][4]
                if box_rfid_three != "no_one" and box_rfid_three is not None:
                    box_rfid_list.append(box_rfid_three)  # 拼接任务中货盒列表
                else:
                    pass
                box_rfid_four = all_data[0][5]
                if box_rfid_four != "no_one" and box_rfid_four is not None:
                    box_rfid_list.append(box_rfid_four)  # 拼接任务中货盒列表
                else:
                    pass
                box_type = all_data[0][6]
                task_track_json = all_data[0][7]
                # 将任务轨迹字符串转为列表
                task_track_list = json.loads(task_track_json)
                task_track_list[-1]["status"] = "1"
                task_track_json_new = json.dumps(task_track_list)
                user_id = all_data[0][8]
                get_date_sql = f"select location_id from lk_location where lk_location_name = '{location_name}'"
                location_id = self.db.get_db_data(get_date_sql)[0][0]
                # 改写所有货盒数据，in_lk，location_name
                if box_rfid_list:
                    for box in box_rfid_list:
                        update_state_sql = f"update wms_goods_box set box_location = %s, location_name = %s, " \
                                           f"location_id = %s, create_time = now(3) where box_rfid = %s"
                        update_param_list = ["in_lk", location_name, location_id, box]
                        self.db.insert_or_update_db_data(update_state_sql, update_param_list)
                        if int(box_type) == 41:
                            get_data_sql = f"select qr_code, material_name from wms_goods_box where box_rfid = '{box}'"
                            get_data = self.db.get_db_data(get_data_sql)
                            if get_data != ():
                                qr_code = get_data[0][0]
                                material_name = get_data[0][1]
                                if qr_code is not None:
                                    update_state_sql = f"update wms_mujv_location set location_name = %s, " \
                                                       f"action_time = now(3) where qr_code = %s"
                                    update_param_list = [location_name, qr_code]
                                    self.db.insert_or_update_db_data(update_state_sql, update_param_list)
                                    get_data_sql = f"select user_code from sys_user where id = '{user_id}'"
                                    get_data = self.db.get_db_data(get_data_sql)
                                    if get_data != ():
                                        user_code = get_data[0][0]
                                    else:
                                        user_code = "P000000"
                                    """模具入库
                                    请求体：
                                    {
                                        "action": "MJReturn",
                                        "data": "{\"mjbm\":\"A04040XX-MZH-I01\",\"PersonID\":\"P047782\"}",
                                        "key": "123456789632147",
                                        "date": "2023-05-12 21:15:09.999"
                                    }
                                    响应体：
                                    {
                                        "result": true,
                                        "resultContent": "模具台账更新成功，汇总更新加工数量10片。",
                                        "resultData": null
                                    }
    
                                    接口地址：http://192.168.10.3:8001/api/Wms/WmsCallbackV2"""
                                    date = str(datetime.now())[:-3]
                                    data_dict = {"mjbm": material_name, "PersonID": user_code}
                                    data_dict_json = json.dumps(data_dict, ensure_ascii=False)
                                    mes_dict = {"date": date, "data": data_dict_json, "action": "MJReturn",
                                                "key": "123456789632147"}
                                    mes_json = json.dumps(mes_dict, ensure_ascii=False)
                                    log = f"任务号{self.wcs_task_number}模具入库上报请求体：{mes_json}"
                                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                                    mes_data = mes_json.encode("utf-8")
                                    url = "http://192.168.10.3:8001/api/Wms/WmsCallbackV2"
                                    result = self.res.request(url, mes_data)
                                    log = f"任务号{self.wcs_task_number}模具入库上报返回{result}"
                                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                                else:
                                    pass
                        else:
                            pass
                    log = f"入库任务终点'{box_rfid_list}'wms_goods_box数据改写完成"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                else:
                    log = f"获取任务中货盒列表失败"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # 改写设备任务结束时间，任务状态为task_end，改写task_track
                if handing_way == "auto":
                    task_result = "task_end"
                else:
                    task_result = "user_end"
                update_data_sql = "update wms_task set srm_task_end_time = now(3), task_state = %s, " \
                                  "task_track = %s where id = %s"
                update_param_list = [task_result, task_track_json_new, task_id]
                self.db.insert_or_update_db_data(update_data_sql, update_param_list)
                log = f"入库任务终点wms_task数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # 将入库库位状态改为full
                update_state_sql = f"update lk_location set location_state = %s, box_type = %s, " \
                                   f"layer_one_box_rfid = %s, layer_two_box_rfid = %s, layer_three_box_rfid = %s, " \
                                   f"layer_four_box_rfid = %s  where lk_location_name = %s"
                update_param_list = ["full", box_type, box_rfid_one, box_rfid_two, box_rfid_three, box_rfid_four,
                                     location_name]
                self.db.insert_or_update_db_data(update_state_sql, update_param_list)
                log = f"入库任务终点立库库位数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            else:
                log = f"数据库查询入库任务终点库位，货盒RFID查询结果为空"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
        except Exception as e:
            log = f"堆垛机or多穿入库结束改写相关数据报错{str(e)}"
            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)

    def move_lk_task_end(self):
        try:
            # 用于入库任务，货盒在入库设备任务完成时所有数据的处理
            # 1、改写wms_task的task_end、line_task_end_time，task_track
            # 2、改写wms_goods_box为free、empty，其余全空
            # 23、改写lk_location为full，所有box_rfid，box_type

            # 按照任务号查询任务数据
            get_data_sql = f"select id, start_site, destination_site, box_rfid_one, box_rfid_two, box_rfid_three, " \
                           f"box_rfid_four, box_type, task_track from wms_task " \
                           f"where wcs_task_number = '{self.wcs_task_number}' order by id desc limit 1"
            all_data = self.db.get_db_data(get_data_sql)
            if all_data != ():
                box_rfid_list = []
                task_id = all_data[0][0]
                start_site = all_data[0][1]
                destination_site = all_data[0][2]
                box_rfid_one = all_data[0][3]
                if box_rfid_one != "no_one" and box_rfid_one is not None:
                    box_rfid_list.append(box_rfid_one)  # 拼接任务中货盒列表
                else:
                    pass
                box_rfid_two = all_data[0][4]
                if box_rfid_two != "no_one" and box_rfid_two is not None:
                    box_rfid_list.append(box_rfid_two)  # 拼接任务中货盒列表
                else:
                    pass
                box_rfid_three = all_data[0][5]
                if box_rfid_three != "no_one" and box_rfid_three is not None:
                    box_rfid_list.append(box_rfid_three)  # 拼接任务中货盒列表
                else:
                    pass
                box_rfid_four = all_data[0][6]
                if box_rfid_four != "no_one" and box_rfid_four is not None:
                    box_rfid_list.append(box_rfid_four)  # 拼接任务中货盒列表
                else:
                    pass
                box_type = all_data[0][7]
                task_track_json = all_data[0][8]
                # 将任务轨迹字符串转为列表
                task_track_list = json.loads(task_track_json)
                task_track_list[-1]["status"] = "1"
                task_track_json_new = json.dumps(task_track_list)
                get_date_sql = f"select location_id from lk_location where lk_location_name = '{destination_site}'"
                location_id = self.db.get_db_data(get_date_sql)[0][0]
                # 改写所有货盒数据，in_lk，location_name
                if box_rfid_list:
                    for box in box_rfid_list:
                        update_state_sql = f"update wms_goods_box set box_location = %s, location_name = %s, " \
                                           f"location_id = %s create_time = now(3) where box_rfid = %s"
                        update_param_list = ["in_lk", destination_site, location_id, box]
                        self.db.insert_or_update_db_data(update_state_sql, update_param_list)
                    log = f"入库任务终点'{box_rfid_list}'wms_goods_box数据改写完成"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                else:
                    log = f"获取任务中货盒列表失败"
                    self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # 改写设备任务结束时间，任务状态为task_end，改写task_track
                update_data_sql = "update wms_task set srm_task_end_time = now(3), task_state = %s, " \
                                  "task_track = %s where id = %s"
                update_param_list = ["task_end", task_track_json_new, task_id]
                self.db.insert_or_update_db_data(update_data_sql, update_param_list)
                log = f"入库任务终点wms_task数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
                # 将出库库位状态改为empty，清空库位在库RFID数据
                update_state_sql = f"update lk_location set location_state = %s, box_type = %s, " \
                                   f"layer_one_box_rfid = %s, layer_two_box_rfid = %s, layer_three_box_rfid = %s, " \
                                   f"layer_four_box_rfid = %s  where lk_location_name = %s"
                update_param_list = ["empty", None, "no_one", "no_one", "no_one", "no_one", start_site]
                self.db.insert_or_update_db_data(update_state_sql, update_param_list)
                # 将入库库位状态改为full
                update_state_sql = f"update lk_location set location_state = %s, box_type = %s, " \
                                   f"layer_one_box_rfid = %s, layer_two_box_rfid = %s, layer_three_box_rfid = %s, " \
                                   f"layer_four_box_rfid = %s  where lk_location_name = %s"
                update_param_list = ["full", box_type, box_rfid_one, box_rfid_two, box_rfid_three, box_rfid_four,
                                     destination_site]
                self.db.insert_or_update_db_data(update_state_sql, update_param_list)
                log = f"入库任务终点立库库位数据改写完成"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
            else:
                log = f"数据库查询入库任务终点库位，货盒RFID查询结果为空"
                self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)
        except Exception as e:
            log = f"堆垛机or多穿入库结束改写相关数据报错{str(e)}"
            self.db.write_log_have_task_number(self.log_name, self.wcs_task_number, log)


class TaskEndReportMes(object):
    def __init__(self, wcs_task_number, db=MySqlHelper(), res=ApiRequest()):
        self.db = db
        self.wcs_task_number = wcs_task_number
        self.res = res

    def robot_task_end_report_mes(self):
        log_name = "WMS任务结束上报"
        url = "http://192.168.10.3:8001/api/Wms/WmsCallbackV2"
        get_task_data_sql = f"select mes_task_number, box_rfid_one, box_rfid_two, box_rfid_three, box_rfid_four " \
                            f"from wms_task where wcs_task_number = '{self.wcs_task_number}' order by id desc limit 1"
        task_data = self.db.get_db_data(get_task_data_sql)
        if task_data != ():
            mes_task_number = task_data[0][0]
            box_rfid_one = task_data[0][1]
            box_rfid_two = task_data[0][2]
            box_rfid_three = task_data[0][3]
            box_rfid_four = task_data[0][4]
            box_rfid_list = [box_rfid_one]
            if box_rfid_two != "no_one":
                box_rfid_list.append(box_rfid_two)
            if box_rfid_three != "no_one":
                box_rfid_list.append(box_rfid_three)
            if box_rfid_four != "no_one":
                box_rfid_list.append(box_rfid_four)
            box_data_list = []
            for i in range(0, len(box_rfid_list)):
                box_rfid = box_rfid_list[i]
                get_box_data_sql = f"select material_id, flakiness, box_list_id, goods_qty from wms_goods_box " \
                                   f"where box_rfid = '{box_rfid}'"
                box_data = self.db.get_db_data(get_box_data_sql)
                if box_data != ():
                    material_id = box_data[0][0]
                    flakiness = box_data[0][1]
                    box_list_id = box_data[0][2]
                    goods_qty = box_data[0][3]
                    box_data_dict = {"Flakiness": flakiness, "PlanBoxID": box_list_id, "Qty": goods_qty,
                                     "rfid": box_rfid,
                                     "lkSeriesNum": i + 1}
                    box_data_list.append(box_data_dict)

                else:
                    log = f"货盒:{box_rfid}查询物料信息失败"
                    self.db.write_log_have_task_number(log_name, self.wcs_task_number, log)
                    return {"code": 500, "msg": f"货盒:{box_rfid}查询物料信息失败"}
            date = str(datetime.now())[:-3]
            data_dict = {"MaterialID": material_id, "ID": mes_task_number, "TaskState": "完成",
                         "TaskDetaileds": box_data_list}
            data_dict_json = json.dumps(data_dict, ensure_ascii=False)
            mes_dict = {"date": date, "data": data_dict_json, "action": "TaskReport", "key": "123456789632147"}
            mes_json = json.dumps(mes_dict, ensure_ascii=False)
            log = f"任务号{self.wcs_task_number}到达终点上报请求体：{mes_json}"
            self.db.write_log_have_task_number(log_name, self.wcs_task_number, log)
            mes_data = mes_json.encode("utf-8")
            result = self.res.request(url, mes_data)
            log = f"任务号{self.wcs_task_number}到达终点上报返回{result}"
            self.db.write_log_have_task_number(log_name, self.wcs_task_number, log)
        else:
            log = f"任务号{self.wcs_task_number}查询任务信息失败"
            self.db.write_log_have_task_number(log_name, self.wcs_task_number, log)

    def reya_task_end_report_mes(self, box_rfid, box_location, material_id, batch_num, box_type,
                                 validity_time, user_code):
        """fenliao = {
            "action": "Save_EquipmentMixture",
            "data": "{\"FactoryCode\":\"IF\",\"Equipment\":\"ZN-122/001#LZ\",\"HhlID\":{\"FRID\":\"E0040150C5BDC7D3\","
                    "\"MixtureID\":\"AD103\",\"BindMixtureBatchNo\":\"AD1300-133-112\","
                    "\"ExpirationDate\":\"2023-01-01 17:00\"},\"UserID\":\"P047782\"}",
            "key": "123456789632147",
            "date": "2023-05-12 21:15:09.999"
        }

        mujv = {
            "action": "Save_EquipmentFrock",
            "data": "{\"FactoryCode\":\"IF\",\"Equipment\":\"ZN-122/001#01\",\"Frock\":{\"FRID\":\"E0040150C5BDC7D3\","
                    "\"Mjbm\":\"A14460XX-MZH-I01\"},\"UserID\":\"P047782\"}",
            "key": "123456789632147",
            "date": "2023-05-12 21:15:09.999"
        }

        gangbei = {
            "action": "Save_EquipmentMaterial",
            "data": "{\"FactoryCode\":\"IF\",\"Equipment\":\"ZN-122/001#01\",
            \"Material\":{\"FRID\":\"E0040150C5BD4CA6\","
                    "\"MaterialID\":\"10230024247\"},\"UserID\":\"P047782\"}",
            "key": "123456789632147",
            "date": "2023-05-12 21:15:09.999"
        }"""
        log_name = "MES热压叫料终点上报"
        url = "http://192.168.10.3:8001/api/Wms/WmsCallbackV2"

        date = str(datetime.now())[:-3]
        if int(box_type) == 31:
            action = "Save_EquipmentMixture"
            data = {"FactoryCode": "IF", "Equipment": box_location,
                    "HhlID": {"FRID": box_rfid, "MixtureID": material_id, "BindMixtureBatchNo": batch_num,
                              "ExpirationDate": validity_time}, "UserID": user_code}
            data_str = json.dumps(data)
        elif int(box_type) == 41:
            action = "Save_EquipmentFrock"
            data = {"FactoryCode": "IF", "Equipment": box_location,
                    "Frock": {"FRID": box_rfid, "Mjbm": material_id}, "UserID": user_code}
            data_str = json.dumps(data)
        elif int(box_type) == 21:
            action = "Save_EquipmentMaterial"
            data = {"FactoryCode": "IF", "Equipment": box_location,
                    "Material": {"FRID": box_rfid, "MaterialID": material_id}, "UserID": user_code}
            data_str = json.dumps(data)
        else:
            return {"code": 500, "msg": f"货盒类型错误:{box_type}"}
        mes_dict = {"action": action, "data": data_str, "key": "123456789632147", "date": date}
        mes_json = json.dumps(mes_dict, ensure_ascii=False)
        log = f"压机绑定上报请求体：{mes_json}"
        self.db.write_log_have_task_number(log_name, self.wcs_task_number, log)
        mes_data = mes_json.encode("utf-8")
        result = self.res.request(url, data=mes_data)
        log = f"任务号{self.wcs_task_number}到达终点上报返回{result}"
        self.db.write_log_have_task_number(log_name, self.wcs_task_number, log)
        return result
