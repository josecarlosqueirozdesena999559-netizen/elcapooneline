import bullexapi.constants as OP_code

def candle_generated_v2(api, message, dict_queue_add):
    if message.get("name") == "candles-generated":
        try:
            msg = message.get("msg", {})
            raw_id = msg.get("active_id")
            active = None
            if raw_id is not None:
                for k, v in OP_code.ACTIVES.items():
                    if v == raw_id or str(v) == str(raw_id):
                        active = k
                        break
            if not active:
                active = str(raw_id)

            if not hasattr(api, "real_time_ticks"):
                api.real_time_ticks = {}
            if "value" in msg and msg["value"] is not None:
                try:
                    api.real_time_ticks[active] = float(msg["value"])
                except Exception:
                    pass

            candles_dict = msg.get("candles", {})
            if isinstance(candles_dict, dict):
                for k, v in candles_dict.items():
                    try:
                        v["active_id"] = raw_id
                        v["at"] = msg.get("at")
                        v["ask"] = msg.get("ask")
                        v["bid"] = msg.get("bid")
                        v["close"] = msg.get("value")
                        v["size"] = int(k)
                        size = int(v["size"])
                        from_ = int(v["from"])
                        if hasattr(api, "real_time_candles_maxdict_table") and active in api.real_time_candles_maxdict_table:
                            if size in api.real_time_candles_maxdict_table[active]:
                                maxdict = api.real_time_candles_maxdict_table[active][size]
                                if hasattr(api, "real_time_candles") and active in api.real_time_candles:
                                    dict_queue_add(api.real_time_candles, maxdict, active, size, from_, v)
                    except Exception:
                        pass

            if hasattr(api, "candle_generated_all_size_check") and active in api.candle_generated_all_size_check:
                api.candle_generated_all_size_check[active] = True
        except Exception:
            pass