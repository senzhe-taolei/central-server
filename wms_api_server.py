import json

from config_info import reya_device_list as reya_device_list


def material_if_can_on_reya(box_rfid: str, box_location: str, db, db_137):
    try:
        if box_location in reya_device_list:
            select_data_sql = f"select box_type, material_name, plan_id from wms_goods_box " \
                              f"where box_rfid = '{box_rfid}'"
            get_data = db_137.get_db_data(select_data_sql)
            if not get_data:
                return {"code": 500, "msg": f"货盒'{box_rfid}'物料信息查询失败"}
            else:
                box_type = int(get_data[0][0])
                material_name = get_data[0][1]
                plan_id = get_data[0][2]
                if box_type == 21:
                    select_data_sql = f"select standard_gangbei_name, standard_gangbei_plan_id from " \
                                      f"reya_production_material where device_name = '{box_location}'"
                    get_data = db.get_db_data(select_data_sql)
                    if get_data:
                        standard_gangbei_name = get_data[0][0]
                        standard_gangbei_plan_id = get_data[0][1]
                        if material_name == standard_gangbei_name and plan_id == standard_gangbei_plan_id:
                            wms_date = {"result": "success"}
                            return {"code": 200, "data": wms_date}
                        else:
                            return {"code": 500,
                                    "msg": f"货盒'{box_rfid}'物料名称'{material_name}'和生产计划id'{plan_id}'与"
                                           f"设备'{box_location}'的生产物料参数'{standard_gangbei_name}'"
                                           f"和'{standard_gangbei_plan_id}'不符"}
                    else:
                        return {"code": 500, "msg": f"设备'{box_location}'的生产物料参数查询失败"}
                elif box_type == 31 and material_name.startswith("A"):  # 是主料
                    select_data_sql = f"select standard_zhuliao_name from reya_production_material " \
                                      f"where device_name = '{box_location}'"
                    get_data = db.get_db_data(select_data_sql)
                    if get_data:
                        standard_zhuliao_name = get_data[0][0]
                        if material_name == standard_zhuliao_name:
                            wms_date = {"result": "success"}
                            return {"code": 200, "data": wms_date}
                        else:
                            return {"code": 500, "msg": f"货盒'{box_rfid}'物料名称'{material_name}'与"
                                                        f"设备'{box_location}'的生产物料参数'{standard_zhuliao_name}不符"}
                    else:
                        return {"code": 500, "msg": f"设备'{box_location}'的生产物料参数查询失败"}
                elif box_type == 31 and material_name.startswith("U"):  # 是辅料
                    select_data_sql = f"select standard_fuliao_name from reya_production_material " \
                                      f"where device_name = '{box_location}'"
                    get_data = db.get_db_data(select_data_sql)
                    if get_data:
                        standard_fuliao_name = get_data[0][0]
                        if material_name == standard_fuliao_name:
                            wms_date = {"result": "success"}
                            return {"code": 200, "data": wms_date}
                        else:
                            return {"code": 500, "msg": f"货盒'{box_rfid}'物料名称'{material_name}'与"
                                                        f"设备'{box_location}'的生产物料参数'{standard_fuliao_name}不符"}
                    else:
                        return {"code": 500, "msg": f"设备'{box_location}'的生产物料参数查询失败"}
                else:
                    return {"code": 500, "msg": f"货盒'{box_rfid}'的货盒类型'{box_type}'不合法"}
        else:
            return {"code": 500, "msg": f"定位地点'{box_location}'不合法"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}


def mujv_if_can_on_reya(material_name: str, material_type: str, location_name: str, db):
    try:
        if location_name in reya_device_list:
            if material_type == "MJ":
                select_data_sql = f"select standard_mj_name from reya_production_material " \
                                  f"where device_name = '{location_name}'"
                get_data = db.get_db_data(select_data_sql)
                if get_data:
                    standard_mj_name = get_data[0][0]
                    if material_name == standard_mj_name:
                        wms_date = {"result": "success"}
                        return {"code": 200, "data": wms_date}
                    else:
                        return {"code": 500, "msg": f"模具'{material_name}'与"
                                                    f"设备'{location_name}'的生产物料参数'{standard_mj_name}'不符"}
                else:
                    return {"code": 500, "msg": f"设备'{location_name}'的生产物料参数查询失败"}
            elif material_type == "TL":  # 是主料
                select_data_sql = f"select standard_tl_name from reya_production_material " \
                                  f"where device_name = '{location_name}'"
                get_data = db.get_db_data(select_data_sql)
                if get_data:
                    standard_tl_name = get_data[0][0]
                    if material_name == standard_tl_name:
                        update_data_sql = f"update reya_production_material set tl_if_on = %s where device_name = %s"
                        update_list = [1, location_name]
                        db.insert_or_update_db_data(update_data_sql, update_list)
                        wms_date = {"result": "success"}
                        return {"code": 200, "data": wms_date}
                    else:
                        return {"code": 500, "msg": f"投料器'{material_name}'与"
                                                    f"设备'{location_name}'的生产物料参数'{standard_tl_name}'不符"}
                else:
                    return {"code": 500, "msg": f"设备'{location_name}'的生产物料参数查询失败"}
            else:
                return {"code": 500, "msg": f"模具或投料器'{material_name}'的物料类型'{material_type}'不合法"}
        else:
            return {"code": 500, "msg": f"定位地点'{location_name}'不合法"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}


def material_on_reya(box_rfid: str, box_location, db, db_137):
    try:
        get_data_sql = f"select box_type, material_name, goods_qty from wms_goods_box where box_rfid = '{box_rfid}'"
        get_data = db_137.get_db_data(get_data_sql)
        if get_data:
            box_type = int(get_data[0][0])
            material_name = get_data[0][1]
            goods_qty = get_data[0][2]
        else:
            return {"code": 500, "msg": f"货盒'{box_rfid}'装盒数量查询失败"}
        device_name_list = []
        if box_location[-2:] in ["01", "02", "03", "04"]:
            device_name_list = [box_location[0: -2] + "01", box_location[0: -2] + "02", box_location[0: -2] + "03",
                                box_location[0: -2] + "04"]
        elif box_location[-2:] in ["05", "06", "07", "08"]:
            device_name_list = [box_location[0: -2] + "05", box_location[0: -2] + "06", box_location[0: -2] + "07",
                                box_location[0: -2] + "08"]

        if box_type == 21:  # 是主料
            get_data_sql = f"select on_device_gangbei_box_list from reya_production_material " \
                           f"where device_name = '{box_location}'"
            get_data = db.get_db_data(get_data_sql)
            if get_data:
                on_device_gangbei_box_list = get_data[0][0]
            else:
                return {"code": 500, "msg": f"货盒'{box_rfid}'装盒数量查询失败"}
            if not on_device_gangbei_box_list:
                on_device_gangbei_box_list = []
            else:
                on_device_gangbei_box_list = json.loads(on_device_gangbei_box_list)
            on_device_gangbei_box_list.append(box_rfid)
            on_device_gangbei_box_list = json.dumps(on_device_gangbei_box_list)
            update_data_sql = f"update reya_production_material set on_device_gangbei_box_list = %s, " \
                              f"on_device_goods_qty = on_device_goods_qty+%s where device_name = %s"
            update_list = [on_device_gangbei_box_list, goods_qty, box_location]
            db.insert_or_update_db_data(update_data_sql, update_list)
            wms_date = {"result": "success"}
            return {"code": 200, "data": wms_date}
        elif box_type == 31 and material_name.startswith("A"):  # 是主料
            if device_name_list:
                for device_name in device_name_list:
                    update_data_sql = f"update reya_production_material set on_device_zhuliao_box = %s, " \
                                      f"on_device_goods_qty = on_device_goods_qty+%s where device_name = %s"
                    update_list = [box_rfid, device_name]
                    db.insert_or_update_db_data(update_data_sql, update_list)
                wms_date = {"result": "success"}
                return {"code": 200, "data": wms_date}
            else:
                return {"code": 500, "msg": f"混合料定位设备列表拼接失败"}
        elif box_type == 31 and material_name.startswith("U"):  # 是主料
            if device_name_list:
                for device_name in device_name_list:
                    update_data_sql = f"update reya_production_material set on_device_fuliao_box = %s, " \
                                      f"on_device_goods_qty = on_device_goods_qty+%s where device_name = %s"
                    update_list = [box_rfid, device_name]
                    db.insert_or_update_db_data(update_data_sql, update_list)
                wms_date = {"result": "success"}
                return {"code": 200, "data": wms_date}
            else:
                return {"code": 500, "msg": f"混合料定位设备列表拼接失败"}
        else:
            return {"code": 500, "msg": f"货盒'{box_rfid}'的货盒类型'{box_type}'不合法"}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}


def mujv_on_reya(material_type: str, location_name: str, db):
    try:
        if material_type == "MJ":
            update_data_sql = f"update reya_production_material set mj_if_on = %s where device_name = %s"
            update_list = [1, location_name]
            db.insert_or_update_db_data(update_data_sql, update_list)

            wms_date = {"result": "success"}
            return {"code": 200, "data": wms_date}
        elif material_type == "TL":
            update_data_sql = f"update reya_production_material set tl_if_on = %s where device_name = %s"
            update_list = [1, location_name]
            db.insert_or_update_db_data(update_data_sql, update_list)

            wms_date = {"result": "success"}
            return {"code": 200, "data": wms_date}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}


def check_reya_mujv(material_name: str, db):
    try:
        get_data_sql = f"select mj_name from reya_robot_action_type " \
                       f"where mj_name = '{material_name}'"
        get_data = db.get_db_data(get_data_sql)
        if not get_data:
            insert_data_sql = f"insert into reya_robot_action_type set mj_name = %s"
            insert_list = [material_name]
            db.insert_or_update_db_data(insert_data_sql, insert_list)
            wms_date = {"result": "success"}
            return {"code": 200, "data": wms_date}
        else:
            wms_date = {"result": "success"}
            return {"code": 200, "data": wms_date}
    except Exception as e:
        return {"code": 500, "msg": f"api_server脚本报错:{str(e)}"}
