import os
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SECURITY_GROUP_NAME = os.getenv("SECURITY_GROUP_NAME", "cluster-k8s-sg")
KEY_PAIR_NAME = os.getenv("KEY_PAIR_NAME", "cluster-key")
MY_IP_CIDR = os.getenv("MY_IP_CIDR")
INSTANCE_TYPE = os.getenv("INSTANCE_TYPE", "t3.medium")

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=AWS_REGION,
)
ec2_client = session.client("ec2")
ec2_resource = session.resource("ec2")
ssm_client = session.client("ssm")


def crear_security_group():
    sg = ec2_client.create_security_group(
        GroupName=SECURITY_GROUP_NAME,
        Description="SG cluster MicroK8s Grafana",
    )
    sg_id = sg["GroupId"]

    ec2_client.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
             "IpRanges": [{"CidrIp": MY_IP_CIDR}]},
            {"IpProtocol": "tcp", "FromPort": 3000, "ToPort": 3000,
             "IpRanges": [{"CidrIp": MY_IP_CIDR}]},
        ],
    )
    print(f"Security Group creado: {sg_id}")
    return sg_id


def crear_key_pair():
    key_pair = ec2_client.create_key_pair(KeyName=KEY_PAIR_NAME)
    pem_path = f"{KEY_PAIR_NAME}.pem"
    with open(pem_path, "w") as f:
        f.write(key_pair["KeyMaterial"])
    os.chmod(pem_path, 0o400)
    print(f"Key pair creado: {pem_path}")
    return pem_path


def obtener_ami_ubuntu():
    param = ssm_client.get_parameter(
        Name="/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
    )
    ami_id = param["Parameter"]["Value"]
    print(f"AMI: {ami_id}")
    return ami_id


def lanzar_instancia(security_group_id, ami_id):
    instances = ec2_resource.create_instances(
        ImageId=ami_id,
        MinCount=1,
        MaxCount=1,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_PAIR_NAME,
        SecurityGroupIds=[security_group_id],
        BlockDeviceMappings=[
            {"DeviceName": "/dev/sda1",
             "Ebs": {"VolumeSize": 30, "VolumeType": "gp3"}}
        ],
        TagSpecifications=[
            {"ResourceType": "instance",
             "Tags": [{"Key": "Name", "Value": "Cluster-Kubernetes"}]}
        ],
    )
    instance = instances[0]
    print(f"Instancia creada: {instance.id}")
    instance.wait_until_running()
    instance.reload()
    print(f"IP publica: {instance.public_ip_address}")
    return instance


# ---------------------------------------------------------------
# Comandos ejecutados por SSH dentro de la instancia:
#
# ssh -i cluster-key.pem ubuntu@<IP_PUBLICA>
#
# sudo snap install microk8s --classic
# sudo usermod -a -G microk8s ubuntu
# sudo chown -f -R ubuntu ~/.kube
# exit
# ssh -i cluster-key.pem ubuntu@<IP_PUBLICA>
#
# sudo snap alias microk8s.kubectl kubectl
# microk8s enable dns helm3
# kubectl get nodes
#
# kubectl apply -f namespace-config.yaml
#
# apiVersion: v1
# kind: Namespace
# metadata:
#   name: observability
# ---
# apiVersion: v1
# kind: ResourceQuota
# metadata:
#   name: observability-quota
#   namespace: observability
# spec:
#   hard:
#     requests.cpu: "1"
#     requests.memory: 1Gi
#     limits.cpu: "2"
#     limits.memory: 3Gi
# ---
# apiVersion: networking.k8s.io/v1
# kind: NetworkPolicy
# metadata:
#   name: observability-default
#   namespace: observability
# spec:
#   podSelector: {}
#   policyTypes: ["Ingress", "Egress"]
#   ingress:
#     - from: [{ podSelector: {} }]
#   egress:
#     - to: []
#       ports: [{ protocol: UDP, port: 53 }, { protocol: TCP, port: 53 }]
#     - to: []
#       ports: [{ protocol: TCP, port: 5432 }]
#
# microk8s helm3 repo add grafana https://grafana.github.io/helm-charts
# microk8s helm3 repo update
#
# microk8s helm3 install my-grafana grafana/grafana --namespace observability \
#   --set resources.requests.cpu=100m \
#   --set resources.requests.memory=128Mi \
#   --set resources.limits.cpu=500m \
#   --set resources.limits.memory=512Mi
#
# kubectl get pods --namespace observability
#
# kubectl get secret --namespace observability my-grafana \
#   -o jsonpath="{.data.admin-user}" | base64 --decode; echo
#
# kubectl get secret --namespace observability my-grafana \
#   -o jsonpath="{.data.admin-password}" | base64 --decode; echo
#
# sudo apt install -y tmux
# tmux new -s grafana
# kubectl port-forward --address 0.0.0.0 svc/my-grafana 3000:80 \
#   --namespace observability
# Ctrl+b, d
#
# URL: http://<IP_PUBLICA>:3000
# ---------------------------------------------------------------


def main():
    sg_id = crear_security_group()
    pem_path = crear_key_pair()
    ami_id = obtener_ami_ubuntu()
    instance = lanzar_instancia(sg_id, ami_id)

    print(f"\nInstance ID: {instance.id}")
    print(f"IP publica: {instance.public_ip_address}")
    print(f"ssh -i {pem_path} ubuntu@{instance.public_ip_address}")


if __name__ == "__main__":
    main()
