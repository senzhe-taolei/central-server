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
        come_from = "UI"
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


if __name__ == '__main__':
    uvicorn.run(app='client_api:app', host="0.0.0.0", port=8090, reload=True)
