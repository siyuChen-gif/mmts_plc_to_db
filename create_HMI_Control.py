import yaml
import os

yaml_file = {
    "plc": {
        "ip": "YOUR PLC IP",
        "rack": 0,
        "slot": 1,
        "db_number": 15,
    },
    "experiment": {
        "temp_high": 20.0,
        "temp_low": -40.0,
        "cycles": 1,
        "idle_warm_min": 10,
        "idle_cold_min": 60,
        "temp_high_limit": 30.0,
        "temp_low_limit": -50.0,
    },
    "execution": {
        "dry_run": True,
    },
    "database": {
        "DB_HOST": "YOUR DATABASE IP",
        "DB_PORT": "YOUR DATABASE PORT",
        "DB_USER": "YOUR DATABASE USER",
        "DB_NAME": "YOUR DATABASE NAME",
        "DB_PASSWORD": "YOUR DATABASE PASSWORD",
    },
    "mqtt": {
        "MQTT_BROKER": "YOUR MQTT BROKER IP",
        "MQTT_PORT": 1883,
        "MQTT_TOPIC": "plc/s7-1200/temperature",
    },
    "institution_abbr": "YOUR INSTITUTION ABBR",
}

config_path = os.path.join(os.getcwd(), "HMI_Control.yml")

with open(config_path, "w", encoding="utf-8") as file:
    yaml.dump(yaml_file, file, sort_keys=False, default_flow_style=False)

print(f"YAML file written to: {config_path}")