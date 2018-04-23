#!/usr/bin/python

DOCUMENTATION = """
module: qubole_cluster
short_description: Manage Qubole clusters
description:
    - Manage Qubole clusters
requirements:
    - "python >= 2.7"
    - "qds-sdk >= 1.9.8"
author: Viktor Berlov
options:
    api_key:
        description:
            - Qubole api key
        required: true
    state:
        description:
            - Qubole clusters states
        required: true
        choices: ['setup', 'get', 'start', 'stop', 'delete']
    cluster_id:
        description:
            - Qubole cluster Id
        required: false
    label:
        description:
            - Qubole cluster labes
        required: false
    presto_version:
        description:
            - Presto version, mandatory for presto clusters
        required: false
    spark_version:
        description:
            - Spark version, mandatory for spark clusters
        required: false
    use_account_compute_creds:
        description:
            - use the accounts compute credentials for all clusters of the account
        required: false
    compute_access_key:
        description:
            - EC2 Access Key
        required: false
    compute_secret_key:
        description:
            - EC2 Secret Key
        required: false
    aws_region:
        description:
            - AWS region in which the cluster is created
        required: false
    aws_preferred_availability_zone:
        description:
            - AWS region in which the cluster is created
        required: false
        default: "Any"
    vpc_id:
        description:
            - ID of the vpc in which the cluster is created
        required: false
    subnet_id:
        description:
            - ID of the subnet in which the cluster is created
        required: false
    bastion_node_public_dns:
        description:
            - Bastion host public DNS name if private subnet is provided for the cluster in a VPC
        required: false
    bastion_node_port:
        description:
            - port of the Bastion node
        required: false
        default: 22
    bastion_node_user:
        description:
            - Bastion node user
        required: false
        default: 'ec2-user'
    master_instance_type:
        description:
            - instance type to use for a cluster master node
        required: false
    slave_instance_type:
        description:
            - instance type to use for a cluster slave nodes
        required: false
    initial_nodes:
        description:
            - number of nodes to start the cluster with
        required: false
    max_nodes:
        description:
            - maximum number of nodes up to which the cluster can be auto-scaled
        required: false
    maximum_bid_price_percentage:
        description:
            - maximum value to bid for Spot Instances. In %
        required: false
    slave_request_type:
        description:
            - request type for the autoscaled slave instances
        required: false
        choices=['ondemand', 'spot']
    fallback_to_ondemand:
        description:
            - Fallback to on-demand nodes if spot nodes could not be obtained
        required: false
    ebs_volume_type:
        description:
            - EBS volume type
        required: false
        choices: ['standard', 'ssd', 'gp2', 'st1', 'sc1']
    ebs_volume_size:
        description:
            - EBS volume size
        required: false
    ebs_volume_count:
        description:
            - number of EBS volumes to attach to each cluster instance
        required: false
    custom_ec2_tags:
        description:
            - additional tags to cluster nodes
        required: false
    use_hadoop2:
        description:
            - Set this parameter value to true for starting Hadoop-2 daemons on a cluster
        required: false
    use_spark:
        description:
            - This is a mandatory setting for a Spark cluster
        required: false
    use_qubole_placement_policy:
        description:
            - Use Qubole Block Placement policy for clusters with spot nodes
        required: false
    encrypted_ephemerals:
        description:
            - Encrypt the ephemeral drives on the instance
        required: false
    customer_ssh_key:
        description:
            - SSH key to use to login to the instances
        required: false
    persistent_security_group:
        description:
            - overrides the account-level security group settings
        required: false
    disallow_cluster_termination:
        description:
            - Prevent auto-termination of the cluster after a prolonged period of disuse
        required: false
    enable_ganglia_monitoring:
        description:
            - Enable Ganglia monitoring for the cluster
        required: false
    node_bootstrap_file:
        description:
            - A file that is executed on every node of the cluster at boot time
        required: false
    idle_cluster_timeout:
        description:
            - terminate cluster if idle for hours
        required: false4
        default=2    
"""
EXAMPLES = '''
- name: create qubole cluster
  qubole_cluster:
    api_key: "{{api_key}}"
    state: create
    cluster_id: "{{cluster_id}}"
    label: ['my_cluster']
    presto_version: 1.2.3
    spark_version: 2.2
    compute_access_key: "{{access_key}}"
    compute_secret_key: "{{secret_key}}"
    aws_region: us-east-1
    aws_preferred_availability_zone: Any
    vpc_id: "{{vpc_id}}"
    subnet_id: "{{subnet_id}}"
    bastion_node_public_dns: 52.0.0.100
    master_instance_type: m1.large
    slave_instance_type: m1.large
    initial_nodes: 2
    max_nodes: 2
    slave_request_type: 'spot'
    fallback_to_ondemand: True
    ebs_volume_type: 'standard'
    ebs_volume_size: 1000
    ebs_volume_count: 1
    custom_ec2_tags:
        tag1: value1
        tag2: value2
    use_hadoop2: False
    use_spark: True
    use_qubole_placement_policy: False
    encrypted_ephemerals: False
    customer_ssh_key: "{{ssh_key}}"
    persistent_security_group: ''
    disallow_cluster_termination: False
    enable_ganglia_monitoring: False
    node_bootstrap_file: 'bootstrap.sh'
    idle_cluster_timeout: 2

'''

from ansible.module_utils.basic import *
from qds_sdk.qubole import Qubole
from qds_sdk.cluster import Cluster


def qb_configure(api_token, api_url):
    return Qubole.configure(api_token=api_token,
                            api_url=api_url)


def cluster_data(module):
    data = dict()
    ec2 = dict()
    node = dict()
    hadoop = dict()
    security = dict()
    if module.params["label"]:
        data["label"] = module.params["label"]
    if module.params["presto_version"]:
        data["presto_version"] = module.params["presto_version"]
    if module.params["spark_version"]:
        data["spark_version"] = module.params["spark_version"]
    if module.params["compute_access_key"]:
        ec2["compute_access_key"] = module.params["compute_access_key"]
    if module.params["compute_secret_key"]:
        ec2["compute_secret_key"] = module.params["compute_secret_key"]
    if module.params["aws_region"]:
        ec2["aws_region"] = module.params["aws_region"]
    if module.params["aws_preferred_availability_zone"]:
        ec2["aws_preferred_availability_zone"] = module.params["aws_preferred_availability_zone"]
    if module.params["vpc_id"]:
        ec2["vpc_id"] = module.params["vpc_id"]
    if module.params["subnet_id"]:
        ec2["subnet_id"] = module.params["subnet_id"]
    if module.params["bastion_node_public_dns"]:
        ec2["bastion_node_public_dns"] = module.params["bastion_node_public_dns"]
    if module.params["bastion_node_port"]:
        ec2["bastion_node_port"] = module.params["bastion_node_port"]
    if module.params["bastion_node_user"]:
        ec2["bastion_node_user"] = module.params["bastion_node_user"]
    if module.params["use_account_compute_creds"]:
        ec2["use_account_compute_creds"] = module.params["use_account_compute_creds"]
    if module.params["master_instance_type"]:
        node["master_instance_type"] = module.params["master_instance_type"]
    if module.params["slave_instance_type"]:
        node["slave_instance_type"] = module.params["slave_instance_type"]
    if module.params["initial_nodes"]:
        node["initial_nodes"] = module.params["initial_nodes"]
    if module.params["max_nodes"]:
        node["max_nodes"] = module.params["max_nodes"]
    if module.params["maximum_bid_price_percentage"]:
        node["stable_spot_instance_settings"] = {"maximum_bid_price_percentage": module.params["maximum_bid_price_percentage"]}
    if module.params["slave_request_type"]:
        node["slave_request_type"] = module.params["slave_request_type"]
    if module.params["fallback_to_ondemand"]:
        node["fallback_to_ondemand"] = module.params["fallback_to_ondemand"]
    if module.params["ebs_volume_type"]:
        node["ebs_volume_type"] = module.params["ebs_volume_type"]
    if module.params["ebs_volume_size"]:
        node["ebs_volume_size"] = module.params["ebs_volume_size"]
    if module.params["ebs_volume_count"]:
        node["ebs_volume_count"] = module.params["ebs_volume_count"]
    if module.params["custom_ec2_tags"]:
        node["custom_ec2_tags"] = module.params["custom_ec2_tags"]
    if module.params["use_hadoop2"]:
        hadoop["use_hadoop2"] = module.params["use_hadoop2"]
    if module.params["use_spark"]:
        hadoop["use_spark"] = module.params["use_spark"]
    if module.params["use_qubole_placement_policy"]:
        hadoop["use_qubole_placement_policy"] = module.params["use_qubole_placement_policy"]
    if module.params["encrypted_ephemerals"]:
        security["encrypted_ephemerals"] = module.params["encrypted_ephemerals"]
    if module.params["customer_ssh_key"]:
        security["ssh_public_key"] = module.params["customer_ssh_key"]
    if module.params["persistent_security_group"]:
        security["persistent_security_group"] = module.params["persistent_security_group"]
    if module.params["disallow_cluster_termination"]:
        data["disallow_cluster_termination"] = module.params["disallow_cluster_termination"]
    if module.params["enable_ganglia_monitoring"]:
        data["enable_ganglia_monitoring"] = module.params["enable_ganglia_monitoring"]
    if module.params["node_bootstrap_file"]:
        data["node_bootstrap_file"] = module.params["node_bootstrap_file"]
    if module.params["idle_cluster_timeout"]:
        data["idle_cluster_timeout"] = module.params["idle_cluster_timeout"]
    if len(ec2) > 0:
        data["ec2_settings"] = ec2
    if len(node) > 0:
        data["node_configuration"] = node
    if len(hadoop) > 0:
        data["hadoop_settings"] = hadoop
    if len(security) > 0:
        data["security_settings"] = security
    return data


def qb_cluster_show(module, cluster_id_label):
    try:
        csw = Cluster.show(cluster_id_label=cluster_id_label)
    except Exception as e:
        module.fail_json(msg=e.message)
    module.exit_json(changed=False, cluster=csw['cluster'])


def qb_cluster_create(module, cluster_info, version="v1.3"):
    try:
        cc = Cluster.create(cluster_info=cluster_info, version=version)
    except Exception as e:
        module.fail_json(msg=e.message)
    module.exit_json(changed=True, cluster=cc)


def qb_cluster_delete(module, cluster_id_label):
    try:
        cd = Cluster.delete(cluster_id_label=cluster_id_label)
    except Exception as e:
        module.fail_json(msg=e.message)
    module.exit_json(changed=True, cluster=cd)


def qb_cluster_start(module, cluster_id_label):
    try:
        cst = Cluster.start(cluster_id_label=cluster_id_label)
    except Exception as e:
        module.fail_json(msg=e.message)
    module.exit_json(changed=True, cluster=cst)


def qb_cluster_terminate(module, cluster_id_label):
    try:
        ct = Cluster.terminate(cluster_id_label=cluster_id_label)
    except Exception as e:
        module.fail_json(msg=e.message)
    module.exit_json(changed=True, cluster=ct)

def run_module():

    module_args = dict(
        api_key=dict(type='str', required=True),
        state=dict(type='str', required=True, choices=['setup', 'get', 'start', 'stop', 'delete']),
        cluster_id=dict(type='str', required=False),
        label=dict(type='list', required=False),
        presto_version=dict(type='str', required=False),
        spark_version=dict(type='str', required=False),
        use_account_compute_creds=dict(type='bool', required=False),
        compute_access_key=dict(type='str', required=False),
        compute_secret_key=dict(type='str', required=False),
        aws_region=dict(type='str', required=False),
        aws_preferred_availability_zone=dict(type='str', required=False, default='Any'),
        vpc_id=dict(type='str', required=False),
        subnet_id=dict(type='str', required=False),
        bastion_node_public_dns=dict(type='str', required=False),
        bastion_node_port=dict(type='int', required=False, default=22),
        bastion_node_user=dict(type='str', required=False, default='ec2-user'),
        master_instance_type=dict(type='str', required=False),
        slave_instance_type=dict(type='str', required=False),
        initial_nodes=dict(type='int', required=False),
        max_nodes=dict(type='int', required=False),
        maximum_bid_price_percentage=dict(type='int', required=False),
        slave_request_type=dict(type='str', required=False, choices=['ondemand', 'spot']),
        fallback_to_ondemand=dict(type='bool', required=False),
        ebs_volume_type=dict(type='str', required=False, choices=['standard', 'ssd', 'gp2', 'st1', 'sc1']),
        ebs_volume_size=dict(type='int', required=False),
        ebs_volume_count=dict(type='int', required=False),
        custom_ec2_tags=dict(type='dict', required=False),
        use_hadoop2=dict(type='bool', required=False),
        use_spark=dict(type='bool', required=False),
        use_qubole_placement_policy=dict(type='bool', required=False),
        encrypted_ephemerals=dict(type='bool', required=False),
        customer_ssh_key=dict(type='str', required=False),
        persistent_security_group=dict(type='str', required=False),
        disallow_cluster_termination=dict(type='bool', required=False),
        enable_ganglia_monitoring=dict(type='bool', required=False),
        node_bootstrap_file=dict(type='str', required=False),
        idle_cluster_timeout=dict(type='int', required=False, default=2)
    )

    result = dict(
        changed=False
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    state = module.params['state']
    api_token = module.params['api_key']
    cluster_id_lable = module.params['cluster_id']

    qb_configure(api_token=api_token,
                 api_url="https://us.qubole.com/api")
    if state == 'setup':
        cluster_info = cluster_data(module)
        qb_cluster_create(module, cluster_info=cluster_info, version="v1.3")
    elif state == 'delete':
        qb_cluster_delete(module, cluster_id_lable)
    elif state == 'start':
        qb_cluster_start(module, cluster_id_lable)
    elif state == 'stop':
        qb_cluster_terminate(module, cluster_id_lable)
    elif state == 'get':
        qb_cluster_show(module, cluster_id_lable)

    if module.check_mode:
       return result

    module.exit_json(**result)

def main():
    run_module()

if __name__ == '__main__':
    main()