from pyflink.table import TableEnvironment

def get_table_environment() -> TableEnvironment:
    from pyflink.table import EnvironmentSettings
    env = EnvironmentSettings.in_streaming_mode()
    return TableEnvironment.create(env)
    