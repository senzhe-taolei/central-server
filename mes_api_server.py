import json

import redis_read_and_write
from config_info import reya_device_list as reya_device_list


def production_plan(data: dict, db):
    """请求体样例：
    request = {
        "production_plan_id": "12345678",
        "work_station": "ZN-122/007#03",
        "material": {"material_name": "A22190NWP/AD103/14.90mm",
                     "plan_id": "100020290", "mj_name": "A18040XX-MZH-I01",
                     "tl_name": "A18040XX-MZH-I01-01", "cavity_number": 5},
        "production": {"主料料号": "AD103", "主料投料量": "67.00",
                       "辅料料号": "UF003", "辅料投料量": "15.00",
                       "上模温度": 155.0,
                       "中模温度01": 150.0, "中模温度02": 0.0,
                       "下模温度01": 155.0, "下模温度02": 0.0, "下模温度03": 0.0, "下模温度04": 0.0,
                       "末次保压": "9.00", "末次保压时间": 175.0, "循环次数": 1.0, "放气距离": 30.0,
                       "一次加压时间": 5.0, "一次排气时间": 10.0, "一次压力": "9.00",
                       "二次加压时间": 0.0, "二次排气时间": 0.0, "二次压力": 0.0,
                       "三次加压时间": 0.0, "三次排气时间": 0.0, "三次压力": 0.0,
                       "四次加压时间": 0.0, "四次排气时间": 0.0, "四次压力": 0.0,
                       "五次加压时间": 0.0, "五次排气时间": 0.0, "五次压力": 0.0,
                       "六次加压时间": 0.0, "六次排气时间": 0.0, "六次压力": 0.0,
                       "七次加压时间": 0.0, "七次排气时间": 0.0, "七次压力": 0.0,
                       "产品厚度": "14.90", "模具装料深度": "51.91",
                       "材料面积": "26.36", "热压简图": "A22190NWP",
                       "产品订单": "P221101ARU005/026", "产品编号": "10230100605",
                       "产品名称": "A22190NWP/AD103/14.90mm",
                       "计划开始时间": "2022-12-07T17:11:49.4634634+08:00",
                       "计划结束时间": "2022-12-07T17:11:49.46347+08:00",
                       "加工数量": 520},
        "drawing": [
            {"name": "A21730NXX.jpg",
             "path": "HTTP://192.168.47.9/FppData/HandWorkFile/PMO21031201371-B/A21730NXX.jpg"}]}"""
    try:
        production_plan_id = data.get("production_plan_id")
        work_station = data.get("work_station")
        if work_station in reya_device_list:
            material_name = data.get("material").get("material_name")
            material_plan_id = data.get("material").get("plan_id")
            mj_name = data.get("material").get("mj_name")
            tl_name = data.get("material").get("tl_name")
            cavity_number = data.get("material").get("cavity_number")
            zhuliao_name = data.get("production").get("主料料号")
            fuliao_name = data.get("production").get("辅料料号")
            plan_qty = data.get("production").get("加工数量")
            production_dict = data.get("production")
            production_dict["物料名称"] = material_name
            production_dict["物料生产计划id"] = material_name
            production_dict["模具名称"] = mj_name
            production_dict["模具腔数"] = cavity_number
            production_dict["投料器名称"] = tl_name
            production_dict["work_station"] = work_station
            production_parameter = json.dumps(production_dict)
            drawing = json.dumps(data.get("drawing"))
            select_data_sql = f"select id from reya_production_plan where production_plan_id = '{production_plan_id}'"
            get_data = db.get_db_data(select_data_sql)
            # 如果设备正在生产中，任务不可下发
            device_plan_id = redis_read_and_write.redis_hget("server", f"device_production_plan:{work_station}",
                                               "plan_id")
            if device_plan_id != "00000":
                return {"code": 500, "msg": f"生产计划'{production_plan_id}'下发目标'{work_station}'"
                                            f"生产计划'{device_plan_id}'未完结"}
            # 如果任务信息不在表格reya_production_plan中，插入
            elif not get_data:
                insert_data_sql = f"insert into reya_production_plan set production_plan_id = %s, work_station = %s, " \
                                  f"material_name = %s, material_plan_id = %s, mj_name = %s, tl_name = %s, " \
                                  f"cavity_number = %s, zhuliao_name = %s, fuliao_name = %s, plan_qty = %s, " \
                                  f"production_parameter = %s, drawing = %s, create_time = now(3)"
                insert_list = [production_plan_id, work_station, material_name, material_plan_id, mj_name, tl_name,
                               cavity_number, zhuliao_name, fuliao_name, plan_qty, production_parameter, drawing]
                db.insert_or_update_db_data(insert_data_sql, insert_list)
                update_data_sql = f"update reya_production_material set standard_mj_name = %s, " \
                                  f"standard_tl_name = %s, standard_zhuliao_name = %s, standard_fuliao_name = %s, " \
                                  f"standard_gangbei_name = %s, standard_gangbei_plan_id = %s where device_name = %s"
                update_list = [mj_name, tl_name, zhuliao_name, fuliao_name, material_name, material_plan_id,
                               work_station]
                db.insert_or_update_db_data(update_data_sql, update_list)
                # 写入对应设备redis的生产计划字典
                redis_read_and_write.redis_hmset(work_station, f"production_plan:{production_plan_id}", production_dict)
                # 写入对应设备redis的设备生产计划字典
                device_dict = {"if_working": "wait", "plan_id": production_plan_id}
                redis_read_and_write.redis_hmset(work_station, f"device_production_plan:{work_station}", device_dict)
                # 写入本地redis的设备生产计划字典
                redis_read_and_write.redis_hmset("server", f"device_production_plan:{work_station}", device_dict)
                mes_date = {"result": "success"}
                return {"code": 200, "data": mes_date}
            elif not redis_read_and_write.redis_hget(work_station,
                                                     f"device_production_plan:{work_station}", "plan_id") \
                    or redis_read_and_write.redis_hget(work_station, f"device_production_plan:{work_station}",
                                                       "plan_id") == "0":
                redis_read_and_write.redis_hmset(work_station, f"production_plan:{production_plan_id}", production_dict)
                device_dict = {"if_working": "wait", "plan_id": production_plan_id}
                redis_read_and_write.redis_hmset(work_station, f"device_production_plan:{work_station}", device_dict)
                mes_date = {"result": "success"}
                return {"code": 200, "data": mes_date}
            elif redis_read_and_write.redis_hget(work_station, f"production_plan:{production_plan_id}",
                                                 "work_station") is None:
                redis_read_and_write.redis_hmset(work_station, f"production_plan:{production_plan_id}", production_dict)
                mes_date = {"result": "success"}
                return {"code": 200, "data": mes_date}
            else:
                return {"code": 500, "msg": f"生产计划'{production_plan_id}'已下发至'{work_station}'不可重复下发"}
        else:
            return {"code": 500, "msg": f"生产计划'{production_plan_id}'下发目标'{work_station}'不合法"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}
