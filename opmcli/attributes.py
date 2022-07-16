class Attributes:
    # ENV Attributes
    env_opensearch_endpoint  = None
    env_opensearch_port  = 443
    env_opensearch_basic_auth  = "no"
    env_opensearch_username  = None
    env_opensearch_password  = None
    env_opensearch_verify_certs  = False
    env_opensearch_use_ssl  = True

    rate_update_interval = 30
    total_observation_time = 0
    mointoring_interval = 1

    debug = False


    def __init__(self):
        pass