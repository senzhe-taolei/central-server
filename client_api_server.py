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
                                  f"on_device_zhuliao_box, on_device_fuliao_box, on_device_goods_qty, finish_goods_qty " \
                                  f"from reya_production_material where device_name = '{work_station}'"
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
                            ui_date = {"result": "success"}
                            return {"code": 200, "data": ui_date}
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

