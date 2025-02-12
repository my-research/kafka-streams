.PHONY: up down stop rm logs

# 백그라운드에서 모든 컨테이너 실행
up:
	docker-compose up -d

# 모든 컨테이너 중지 및 네트워크 제거
down:
	docker-compose down

# 실행 중인 컨테이너만 중지
stop:
	docker-compose stop

# 중지된 컨테이너를 강제로 삭제
rm:
	docker-compose rm -f

# 모든 컨테이너 로그를 실시간으로 확인 (선택사항)
logs:
	docker-compose logs -f