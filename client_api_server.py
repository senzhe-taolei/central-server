import json

from config_info import reya_device_list as reya_device_list, reya_device_ip_dict as reya_device_ip_dict
from universal_method import ApiRequest
import redis_read_and_write as r


def reya_production_begin(work_station: str, db):
    try:
        if work_station in reya_device_list:
            if_working = r.redis_hget(work_station, f"device_production_plan:{work_station}", "if_working")
            if if_working == "wait":
                select_data_sql = f"select production_plan_id_now, mj_if_on, tl_if_on, on_device_gangbei_box_list, " \
                                  f"on_device_zhuliao_box, on_device_fuliao_box, on_device_goods_qty, " \
                                  f"finish_goods_qty from reya_production_material where device_name = '{work_station}'"
                get_data = db.get_db_data(select_data_sql)
                if get_data:
                    production_plan_id = get_data[0][0]
                    mj_if_on = get_data[0][1]
                    tl_if_on = get_data[0][2]
                    on_device_gangbei_box_list = get_data[0][3]
                    on_device_zhuliao_box = get_data[0][4]
                    on_device_fuliao_box = get_data[0][5]
                    on_device_goods_qty = get_data[0][6]
                    finish_goods_qty = get_data[0][7]
                    if mj_if_on == 0:
                        return {"code": 500,
                                "msg": f"设备'{work_station}'没有绑定模具"}
                    elif tl_if_on == 0:
                        return {"code": 500,
                                "msg": f"设备'{work_station}'没有绑定投料器"}
                    elif not on_device_gangbei_box_list:
                        return {"code": 500,
                                "msg": f"设备'{work_station}'没有绑定钢背"}
                    elif not on_device_zhuliao_box:
                        return {"code": 500,
                                "msg": f"设备'{work_station}'没有绑定主料"}
                    elif not on_device_fuliao_box:
                        return {"code": 500,
                                "msg": f"设备'{work_station}'没有绑定辅料"}
                    elif on_device_goods_qty == 0:
                        return {"code": 500,
                                "msg": f"设备'{work_station}'绑定钢背数量为0"}
                    elif on_device_goods_qty - finish_goods_qty == 0:
                        return {"code": 500,
                                "msg": f"设备'{work_station}'钢背可用数量为0"}
                    else:
                        mj_name = r.redis_hget(work_station, f"production_plan:{production_plan_id}", "模具名称")
                        select_data_sql = f"select robot_action_type from reya_robot_action_type " \
                                          f"where mj_name = '{mj_name}'"
                        get_data = db.get_db_data(select_data_sql)
                        if get_data:
                            robot_action_type = get_data[0][0]
                            if robot_action_type:
                                r.redis_hset(work_station, f"production_plan:{production_plan_id}",
                                             "模具类型", robot_action_type)
                            else:
                                return {"code": 500,
                                        "msg": f"获取生产所需模具'{mj_name}'的模具类型未设置"}
                        else:
                            return {"code": 500,
                                    "msg": f"获取生产所需模具'{mj_name}'信息失败"}
                        res = ApiRequest()
                        ip = reya_device_ip_dict.get(work_station)
                        url = f"http://{ip}:8090/wms/mujv_on_reya"
                        cc_dict = {"work_station": work_station, "production_plan_id": production_plan_id}
                        cc_json = json.dumps(cc_dict, ensure_ascii=False)
                        cc_data = cc_json.encode("utf-8")
                        result = res.request(url, data=cc_data)
                        if result.get("code") == 200:
                            client_data = {"result": "success"}
                            return {"code": 200, "data": client_data}
                        else:
                            error = result.get("msg")
                            pda_msg = f"任务下发设备返回报错：{error}"
                            return {"code": 500, "msg": pda_msg}
                else:
                    return {"code": 500,
                            "msg": f"设备'{work_station}'生产物料数据查询失败"}
            else:
                return {"code": 500,
                        "msg": f"设备'{work_station}'工作状态'{if_working}'不合法"}
        else:
            return {"code": 500, "msg": f"参数'{work_station}'不合法"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}


def logistics_monitoring(production_plan_id: str, db, db_137):
    try:
        select_data_sql = f"select material_name, material_plan_id from reya_production_material " \
                          f"where production_plan_id = '{production_plan_id}'"
        get_data = db.get_db_data(select_data_sql)
        if get_data:
            material_name = get_data[0][0]
            material_plan_id = get_data[0][1]
            select_data_sql = f"select box_rfid, goods_qty, out_lk_task_id from wms_goods_in_box " \
                              f"where material_name = '{material_name}' and plan_id = '{material_plan_id} " \
                              f"order by id desc"
            get_data = db_137.get_db_data(select_data_sql)
            if get_data:
                box_rfid_list = []
                logistics_monitoring_doct = {}
                for i in get_data:
                    box_rfid = i[0]
                    goods_qty = i[1]
                    out_lk_task_id = i[2]
                    if not goods_qty:
                        goods_qty = 0
                    else:
                        pass
                    if box_rfid not in box_rfid_list:
                        box_rfid_list.append(box_rfid)
                        if not out_lk_task_id:
                            logistics_monitoring_doct[box_rfid] = {"goods_qty": goods_qty, "task_number": "00000",
                                                                   "outing": 0, "on_line": 0, "line_end": 0,
                                                                   "on_device": 0}
                        else:
                            select_data_sql = f"select wcs_task_number, task_state from wms_task " \
                                              f"where id = {out_lk_task_id}"
                            get_data = db_137.get_db_data(select_data_sql)
                            if get_data:
                                wcs_task_number = get_data[0][0]
                                task_state = get_data[0][1]
                                if task_state == "user_cancel" or task_state == "task_cancel":
                                    outing = 0
                                    on_line = 0
                                    line_end = 0
                                    on_device = 0
                                elif task_state == "on_srm":
                                    outing = 1
                                    on_line = 0
                                    line_end = 0
                                    on_device = 0
                                elif task_state == "on_line":
                                    outing = 1
                                    on_line = 1
                                    line_end = 0
                                    on_device = 0
                                elif task_state == "line_task_end":
                                    outing = 1
                                    on_line = 1
                                    line_end = 1
                                    on_device = 0
                                elif task_state == "task_end":
                                    outing = 1
                                    on_line = 1
                                    line_end = 1
                                    on_device = 1
                                else:
                                    wcs_task_number = "88888"
                                    outing = 0
                                    on_line = 0
                                    line_end = 0
                                    on_device = 0
                                logistics_monitoring_doct[box_rfid] = {"goods_qty": goods_qty,
                                                                       "task_number": wcs_task_number,
                                                                       "outing": outing, "on_line": on_line,
                                                                       "line_end": line_end,
                                                                       "on_device": on_device}
                            else:
                                logistics_monitoring_doct[box_rfid] = {"goods_qty": goods_qty, "task_number": "99999",
                                                                       "outing": 0, "on_line": 0, "line_end": 0,
                                                                       "on_device": 0}
                    else:
                        pass
                client_data = logistics_monitoring_doct
                return {"code": 200, "data": client_data}

            else:
                return {"code": 500,
                        "msg": f"生产计划'{production_plan_id}'可用物料为空"}
        else:
            return {"code": 500,
                    "msg": f"生产计划'{production_plan_id}'查询物料信息失败"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}


def reya_get_gangbei_out_lk(work_station: str, production_plan_id: str, box_qty: int, db, db_137):
    try:
        select_data_sql = f"select material_name, material_plan_id from reya_production_material " \
                          f"where production_plan_id = '{production_plan_id}'"
        get_data = db.get_db_data(select_data_sql)
        if get_data:
            material_name = get_data[0][0]
            material_plan_id = get_data[0][1]
            select_data_sql = f"select box_rfid, location_name from wms_goods_box " \
                              f"where material_name = '{material_name}' and plan_id = '{material_plan_id} " \
                              f"and box_type = 21 and box_location = 'in_lk' limit {box_qty}"
            get_data = db_137.get_db_data(select_data_sql)
            if get_data:
                box_rfid_list = []
                box_rfid_dict = {}
                for i in get_data:
                    box_rfid = i[0]
                    location_name = i[1]
                    if box_rfid not in box_rfid_list:
                        box_rfid_list.append(box_rfid)
                        box_rfid_dict[location_name] = box_rfid
                    else:
                        pass
                res = ApiRequest()
                url = f"http://192.168.10.137:8080/cc/reya_get_material_out_lk"
                cc_dict = {"work_station": work_station, "box_rfid_dict": box_rfid_dict}
                cc_json = json.dumps(cc_dict, ensure_ascii=False)
                cc_data = cc_json.encode("utf-8")
                result = res.request(url, data=cc_data)
                if result.get("code") == 200:
                    client_data = {"result": "success"}
                    return {"code": 200, "data": client_data}
                else:
                    error = result.get("msg")
                    pda_msg = f"叫料任务下发返回报错：{error}"
                    return {"code": 500, "msg": pda_msg}
            else:
                return {"code": 500,
                        "msg": f"生产计划'{production_plan_id}'所有在库钢背已全部出库"}
        else:
            return {"code": 500,
                    "msg": f"生产计划'{production_plan_id}'查询物料信息失败"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}


def reya_get_fenliao_out_lk(work_station: str, production_plan_id: str, fenliao_type: str, db_137):
    try:
        if fenliao_type == "zhuliao":
            material_name = r.redis_hget(work_station, f"production_plan:{production_plan_id}", "主料料号")
        elif fenliao_type == "fuliao":
            material_name = r.redis_hget(work_station, f"production_plan:{production_plan_id}", "辅料料号")
        else:
            return {"code": 500, "msg": f"生产计划'{production_plan_id}'混合料叫料粉料类型'{fenliao_type}'不合法"}
        select_data_sql = f"select box_rfid, location_name from wms_goods_box " \
                          f"where material_name = '{material_name}' and box_type = 31 and box_location = 'in_lk' " \
                          f"order by create_time limit 1"
        get_data = db_137.get_db_data(select_data_sql)
        if get_data:
            box_rfid = get_data[0][0]
            location_name = get_data[0][1]
            box_rfid_dict = {location_name: box_rfid}
            res = ApiRequest()
            url = f"http://192.168.10.137:8080/cc/reya_get_material_out_lk"
            cc_dict = {"work_station": work_station, "box_rfid_dict": box_rfid_dict}
            cc_json = json.dumps(cc_dict, ensure_ascii=False)
            cc_data = cc_json.encode("utf-8")
            result = res.request(url, data=cc_data)
            if result.get("code") == 200:
                client_data = {"result": "success"}
                return {"code": 200, "data": client_data}
            else:
                error = result.get("msg")
                pda_msg = f"叫料任务下发返回报错：{error}"
                return {"code": 500, "msg": pda_msg}
        else:
            return {"code": 500,
                    "msg": f"生产计划'{production_plan_id}'所有在库主料或辅料已全部出库"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}
