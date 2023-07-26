import uvicorn
from fastapi import FastAPI, status, Body
import json

import wms_api_server
from db_operation import MySqlHelper

app = FastAPI()


# WMS绑定货盒，查询货盒与生产计划是否相符
@app.post("/wms/material_if_can_on_reya", status_code=status.HTTP_200_OK)
def material_if_can_on_reya(data=Body()):
    db = MySqlHelper()
    db_137 = MySqlHelper(host="137")
    try:
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
        come_from = "WMS"
        go_to = "CC"
        action = "压机绑定物料"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        box_rfid = data.get("box_rfid")
        box_location = data.get("box_location")
        if not box_rfid or not box_location:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = wms_api_server.material_if_can_on_reya(box_rfid, box_location, db, db_137)

            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
    except Exception as e:
        return {"code": 500, "msg": f"接口异常{str(e)}"}
    finally:
        db.close_db()
        db_137.close_db()


# WMS绑定模具投料器，查询与生产计划是否相符
@app.post("/wms/mujv_if_can_on_reya", status_code=status.HTTP_200_OK)
def mujv_if_can_on_reya(data=Body()):
    db = MySqlHelper()
    try:
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
        come_from = "WMS"
        go_to = "CC"
        action = "压机绑定物料"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        material_name = data.get("material")
        material_type = data.get("material_type")
        location_name = data.get("location_name")
        if not material_name or not material_type or not location_name:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = wms_api_server.mujv_if_can_on_reya(material_name, material_type, location_name, db)

            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
    except Exception as e:
        return {"code": 500, "msg": f"接口异常{str(e)}"}
    finally:
        db.close_db()


# WMS绑定货盒，改写设备物料表数据
@app.post("/wms/material_on_reya", status_code=status.HTTP_200_OK)
def material_on_reya(data=Body()):
    db = MySqlHelper()
    db_137 = MySqlHelper(host="137")
    try:
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
        come_from = "WMS"
        go_to = "CC"
        action = "压机绑定物料"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        box_rfid = data.get("box_rfid")
        box_location = data.get("box_location")
        if not box_rfid or not box_location:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = wms_api_server.material_on_reya(box_rfid, box_location, db, db_137)

            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
    except Exception as e:
        return {"code": 500, "msg": f"接口异常{str(e)}"}
    finally:
        db.close_db()
        db_137.close_db()


# WMS绑定模具，改写设备物料表数据
@app.post("/wms/material_on_reya", status_code=status.HTTP_200_OK)
def material_on_reya(data=Body()):
    db = MySqlHelper()
    try:
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
        come_from = "WMS"
        go_to = "CC"
        action = "压机绑定物料"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        material_type = data.get("material_type")
        location_name = data.get("location_name")
        if not material_type or not location_name:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = wms_api_server.mujv_on_reya(material_type, location_name, db)

            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
    except Exception as e:
        return {"code": 500, "msg": f"接口异常{str(e)}"}
    finally:
        db.close_db()


# WMS重新装盒时，更新热压模具数据
@app.post("/wms/check_reya_mujv", status_code=status.HTTP_200_OK)
def check_reya_mujv(data=Body()):
    db = MySqlHelper()
    try:
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
        come_from = "WMS"
        go_to = "CC"
        action = "压机绑定物料"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        material_name = data.get("material_name")
        if not material_name:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = wms_api_server.check_reya_mujv(material_name, db)

            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
    except Exception as e:
        return {"code": 500, "msg": f"接口异常{str(e)}"}
    finally:
        db.close_db()


if __name__ == '__main__':
    uvicorn.run(app='wms_api:app', host="0.0.0.0", port=8070, reload=True)
