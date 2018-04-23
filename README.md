# qubole_ansible_modules
Qubole ansible modules

# project overview
Qubole Data Services Ansible modules.
Completed modules:
- qubole_cluster

# installation
Module requires qds-sdk python pakage
```
pip install -r requirements.yml
```
install qubole modules
```
ansible-galaxy install git+https://github.com/nrgene/qubole_ansible_modules.git
```
# testing
Get the latest code
```
git clone git@github.com:nrgene/qubole_ansible_modules.git
```
Run test playbook.
Create cluster:
```
cd qubole_ansible_modules/
ansible-playbook test.yml -e api_key=xyxyxyxyxyxyxyxyxyxyxy -e state=setup
```
Delete cluster:
```
ansible-playbook test.yml -e api_key=xyxyxyxyxyxyxyxyxyxyxy -e state=delete -e cluster_id=test-ansible-module-cluster
```