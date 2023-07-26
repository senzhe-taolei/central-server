import uvicorn
from fastapi import FastAPI, status, Body
import json

import client_api_server
from db_operation import MySqlHelper

app = FastAPI()


# 热压中控客户端压机任务启动按钮
@app.post("/client/reya_production_begin", status_code=status.HTTP_200_OK)
def reya_production_begin(data=Body()):
    db = MySqlHelper()
    try:
        come_from = "Client"
        go_to = "CC"
        action = "压机任务执行"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        work_station = data.get("work_station")
        if not work_station:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = client_api_server.reya_production_begin(work_station, db)

            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
    except Exception as e:
        return {"code": 500, "msg": f"接口异常{str(e)}"}
    finally:
        db.close_db()


# 热压中控客户端物流监控按钮
@app.post("/client/logistics_monitoring", status_code=status.HTTP_200_OK)
def logistics_monitoring(data=Body()):
    db = MySqlHelper()
    db_137 = MySqlHelper(host="137")
    try:
        come_from = "Client"
        go_to = "CC"
        action = "物流监控"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        production_plan_id = data.get("production_plan_id")
        if not production_plan_id:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = client_api_server.logistics_monitoring(production_plan_id, db, db_137)

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


# 热压中控客户端钢背叫料按钮
@app.post("/client/reya_get_gangbei_out_lk", status_code=status.HTTP_200_OK)
def reya_get_gangbei_out_lk(data=Body()):
    db = MySqlHelper()
    db_137 = MySqlHelper(host="137")
    try:
        come_from = "Client"
        go_to = "CC"
        action = "物流监控"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        production_plan_id = data.get("production_plan_id")
        work_station = data.get("work_station")
        box_qty = data.get("box_qty")
        if not production_plan_id:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = client_api_server.reya_get_gangbei_out_lk(work_station, production_plan_id, int(box_qty),
                                                                         db, db_137)

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


# 热压中控客户端主料叫料和辅料叫料按钮
@app.post("/client/reya_get_fenliao_out_lk", status_code=status.HTTP_200_OK)
def reya_get_fenliao_out_lk(data=Body()):
    db = MySqlHelper()
    db_137 = MySqlHelper(host="137")
    try:
        come_from = "Client"
        go_to = "CC"
        action = "物流监控"
        request_log = json.dumps(data)
        insert_data_sql = "insert into api_log(come_from, go_to, action, request_log, create_time) " \
                          "values(%s, %s, %s, %s, now(3))"
        param_list = [come_from, go_to, action, request_log]
        insert_id = db.insert_or_update_db_data(insert_data_sql, param_list)
        production_plan_id = data.get("production_plan_id")
        work_station = data.get("work_station")
        fenliao_type = data.get("fenliao_type")
        if not production_plan_id:
            return_data_dict = {"code": 500, "msg": f"参数有空值"}
            return_data_json = json.dumps(return_data_dict, ensure_ascii=False)
            update_data_sql = f"update api_log set return_log=%s where id=%s"
            update_param_list = [return_data_json, int(insert_id)]
            db.insert_or_update_db_data(update_data_sql, update_param_list)
            return return_data_dict
        else:
            return_data_dict = client_api_server.reya_get_fenliao_out_lk(work_station, production_plan_id, fenliao_type,
                                                                         db_137)

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


if __name__ == '__main__':
    uvicorn.run(app='client_api:app', host="0.0.0.0", port=8090, reload=True)
