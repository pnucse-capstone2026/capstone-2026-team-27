#!/usr/bin/env bash
# scripts/recreate_test_resources.sh
#
# CloudWatch 연동 테스트용 AWS 리소스(EC2, Lambda, AutoScaling)를 재생성한다.
# 2026-08-25에 수동으로 만들었던 것과 동일한 사양으로 다시 만들되, 이미 존재하는
# 것(IAM 역할, 런치 템플릿 등 비용이 안 드는 것들)은 건드리지 않고 재사용한다
# (여러 번 실행해도 안전 — 이미 있으면 스킵).
#
# 실행 전 확인:
#   aws sts get-caller-identity   # 자격증명/계정 확인
#
# 실행 방법:
#   bash scripts/recreate_test_resources.sh
#
# 끝나고 나서: 출력된 INSTANCE_ID를 .env의 INSTANCE_ID에 반영할 것
# (LAMBDA_FUNCTION_NAME=detection-test-lambda, ASG_NAME=detection-test-asg는 이름 고정이라 안 바뀜)

set -euo pipefail

REGION="ap-northeast-2"
TAG_KEY="Detection"
TAG_VALUE="true"

LAMBDA_FUNCTION_NAME="detection-test-lambda"
LAMBDA_ROLE_NAME="detection-test-lambda-role"

ASG_NAME="detection-test-asg"
LAUNCH_TEMPLATE_NAME="detection-test-lt"

SSM_ROLE_NAME="detection-test-ec2-ssm-role"
SSM_PROFILE_NAME="detection-test-ec2-ssm-profile"

echo "=================================================================="
echo "0. 사전 조회 (VPC / 서브넷 / 보안그룹 / 최신 AMI)"
echo "=================================================================="

VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" \
  --query "Vpcs[0].VpcId" --output text)
echo "기본 VPC: $VPC_ID"

SUBNET_ID=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=default-for-az,Values=true" \
  --query "Subnets[0].SubnetId" --output text)
echo "서브넷: $SUBNET_ID"

SECURITY_GROUP_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=default" "Name=vpc-id,Values=$VPC_ID" \
  --query "SecurityGroups[0].GroupId" --output text)
echo "보안그룹: $SECURITY_GROUP_ID"

AMI_ID=$(aws ec2 describe-images --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-x86_64" "Name=state,Values=available" \
  --query "sort_by(Images, &CreationDate)[-1].ImageId" --output text)
echo "최신 Amazon Linux 2023 AMI: $AMI_ID"


echo ""
echo "=================================================================="
echo "1. EC2 인스턴스 생성"
echo "=================================================================="

INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type t3.micro \
  --subnet-id "$SUBNET_ID" \
  --security-group-ids "$SECURITY_GROUP_ID" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG_KEY,Value=$TAG_VALUE},{Key=Name,Value=detection-test-ec2}]" \
  --count 1 \
  --query "Instances[0].InstanceId" --output text)
echo "EC2 인스턴스 생성됨: $INSTANCE_ID"


echo ""
echo "=================================================================="
echo "2. EC2용 SSM IAM 역할 + 인스턴스 프로파일 (CPU 부하 테스트용)"
echo "=================================================================="

if aws iam get-role --role-name "$SSM_ROLE_NAME" >/dev/null 2>&1; then
  echo "SSM 역할($SSM_ROLE_NAME) 이미 존재 — 재사용"
else
  aws iam create-role \
    --role-name "$SSM_ROLE_NAME" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    --tags "Key=$TAG_KEY,Value=$TAG_VALUE" >/dev/null
  aws iam attach-role-policy \
    --role-name "$SSM_ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
  echo "SSM 역할 생성 완료: $SSM_ROLE_NAME"
fi

if aws iam get-instance-profile --instance-profile-name "$SSM_PROFILE_NAME" >/dev/null 2>&1; then
  echo "인스턴스 프로파일($SSM_PROFILE_NAME) 이미 존재 — 재사용"
else
  aws iam create-instance-profile --instance-profile-name "$SSM_PROFILE_NAME" >/dev/null
  aws iam add-role-to-instance-profile \
    --instance-profile-name "$SSM_PROFILE_NAME" \
    --role-name "$SSM_ROLE_NAME"
  echo "인스턴스 프로파일 생성 완료: $SSM_PROFILE_NAME"
  echo "IAM 전파 대기 (10초)..."
  sleep 10
fi

aws ec2 associate-iam-instance-profile \
  --instance-id "$INSTANCE_ID" \
  --iam-instance-profile "Name=$SSM_PROFILE_NAME" >/dev/null
echo "인스턴스 프로파일을 $INSTANCE_ID 에 연결함"

echo "SSM 에이전트가 새 권한을 인식하도록 재부팅..."
aws ec2 reboot-instances --instance-ids "$INSTANCE_ID" >/dev/null

echo "SSM 등록 대기 중 (최대 5분)..."
for i in $(seq 1 20); do
  sleep 15
  STATUS=$(aws ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
    --query "InstanceInformationList[0].PingStatus" --output text 2>/dev/null || echo "None")
  if [ "$STATUS" == "Online" ]; then
    echo "SSM 등록 완료! (${i}x15초)"
    break
  fi
done


echo ""
echo "=================================================================="
echo "3. Lambda 함수"
echo "=================================================================="

if aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" >/dev/null 2>&1; then
  echo "Lambda 함수($LAMBDA_FUNCTION_NAME) 이미 존재 — 재사용 (재생성 안 함)"
else
  if aws iam get-role --role-name "$LAMBDA_ROLE_NAME" >/dev/null 2>&1; then
    echo "Lambda 역할($LAMBDA_ROLE_NAME) 이미 존재 — 재사용"
  else
    aws iam create-role \
      --role-name "$LAMBDA_ROLE_NAME" \
      --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
      --tags "Key=$TAG_KEY,Value=$TAG_VALUE" >/dev/null
    aws iam attach-role-policy \
      --role-name "$LAMBDA_ROLE_NAME" \
      --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    echo "Lambda 역할 생성 완료. IAM 전파 대기 (10초)..."
    sleep 10
  fi

  LAMBDA_ROLE_ARN=$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query "Role.Arn" --output text)

  TMP_DIR=$(mktemp -d)
  cat > "$TMP_DIR/lambda_function.py" << 'PYEOF'
def lambda_handler(event, context):
    return {"statusCode": 200, "body": "detection test lambda ok"}
PYEOF
  (cd "$TMP_DIR" && zip -q lambda_pkg.zip lambda_function.py)

  aws lambda create-function \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --runtime python3.12 \
    --role "$LAMBDA_ROLE_ARN" \
    --handler lambda_function.lambda_handler \
    --zip-file "fileb://$TMP_DIR/lambda_pkg.zip" \
    --memory-size 128 \
    --timeout 10 \
    --tags "$TAG_KEY=$TAG_VALUE" >/dev/null
  rm -rf "$TMP_DIR"
  echo "Lambda 함수 생성 완료: $LAMBDA_FUNCTION_NAME"
fi


echo ""
echo "=================================================================="
echo "4. AutoScaling (런치 템플릿 + 그룹)"
echo "=================================================================="

if aws ec2 describe-launch-templates --launch-template-names "$LAUNCH_TEMPLATE_NAME" >/dev/null 2>&1; then
  echo "런치 템플릿($LAUNCH_TEMPLATE_NAME) 이미 존재 — 재사용"
else
  aws ec2 create-launch-template \
    --launch-template-name "$LAUNCH_TEMPLATE_NAME" \
    --launch-template-data "{
      \"ImageId\": \"$AMI_ID\",
      \"InstanceType\": \"t3.micro\",
      \"SecurityGroupIds\": [\"$SECURITY_GROUP_ID\"],
      \"TagSpecifications\": [{\"ResourceType\":\"instance\",\"Tags\":[{\"Key\":\"$TAG_KEY\",\"Value\":\"$TAG_VALUE\"},{\"Key\":\"Name\",\"Value\":\"detection-test-asg-instance\"}]}]
    }" >/dev/null
  echo "런치 템플릿 생성 완료: $LAUNCH_TEMPLATE_NAME"
fi

if aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$ASG_NAME" \
  --query "AutoScalingGroups[0]" --output text | grep -q "None"; then
  aws autoscaling create-auto-scaling-group \
    --auto-scaling-group-name "$ASG_NAME" \
    --launch-template "LaunchTemplateName=$LAUNCH_TEMPLATE_NAME,Version=\$Latest" \
    --min-size 1 --max-size 2 --desired-capacity 1 \
    --vpc-zone-identifier "$SUBNET_ID" \
    --tags "Key=$TAG_KEY,Value=$TAG_VALUE,PropagateAtLaunch=true"
  aws autoscaling enable-metrics-collection \
    --auto-scaling-group-name "$ASG_NAME" \
    --granularity "1Minute" \
    --metrics "GroupDesiredCapacity" "GroupInServiceInstances"
  echo "AutoScaling 그룹 생성 + Group Metrics Collection 활성화 완료: $ASG_NAME"
else
  echo "AutoScaling 그룹($ASG_NAME) 이미 존재 — 재사용"
fi


echo ""
echo "=================================================================="
echo "완료"
echo "=================================================================="
echo "EC2_INSTANCE_ID   = $INSTANCE_ID"
echo "LAMBDA_FUNCTION_NAME = $LAMBDA_FUNCTION_NAME"
echo "ASG_NAME          = $ASG_NAME"
echo ""
echo "⚠️  .env 파일의 INSTANCE_ID를 위 값으로 갱신하세요:"
echo "    INSTANCE_ID=$INSTANCE_ID"
