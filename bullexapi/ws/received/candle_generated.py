"""Module for Bullex websocket."""
import bullexapi.constants as OP_code
import bullexapi.global_value as global_value

def candle_generated_realtime(api, message, dict_queue_add):
    if message.get("name") == "candle-generated":
        try:
            msg = message.get("msg", {})
            raw_id = msg.get("active_id")
            active = None
            if raw_id is not None:
                # Procura por ID numérico ou string
                for k, v in OP_code.ACTIVES.items():
                    if v == raw_id or str(v) == str(raw_id):
                        active = k
                        break
            if not active:
                active = str(raw_id)

            size = int(msg.get("size", 1))
            from_ = int(msg.get("from", 0))

            if not hasattr(api, "real_time_ticks"):
                api.real_time_ticks = {}
            if "close" in msg and msg["close"] is not None:
                try:
                    c_val = float(msg["close"])
                    api.real_time_ticks[active] = c_val
                except Exception:
                    pass

            if hasattr(api, "real_time_candles_maxdict_table") and active in api.real_time_candles_maxdict_table:
                if size in api.real_time_candles_maxdict_table[active]:
                    maxdict = api.real_time_candles_maxdict_table[active][size]
                    if hasattr(api, "real_time_candles") and active in api.real_time_candles:
                        dict_queue_add(api.real_time_candles, maxdict, active, size, from_, msg)
            if hasattr(api, "candle_generated_check") and active in api.candle_generated_check:
                api.candle_generated_check[active][size] = True
        except Exception:
            pass