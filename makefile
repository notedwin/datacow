run:
	python3 aw2postgres.py 
	python3 caddy2postgres.py
	python3 getIPdata.py
	python3 recent_listening.py
	python3 cal.py

web:
	python3 websites_ip.py