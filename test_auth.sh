#!/bin/bash
export QT_QPA_PLATFORM=offscreen

# Start manager with a secret modified temporarily
sed -i 's/MANAGER_SECRET = ""/MANAGER_SECRET = "test_secret"/' AE_RenderManager.py
python3 AE_RenderManager.py >/dev/null 2>&1 &
MGR_PID=$!
sleep 4

# Test 1: No secret
echo "Test 1: No secret (expect 401)"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d '{"dummy": "data"}' http://127.0.0.1:9876/submit)
echo "Result: $HTTP_CODE"

# Test 2: Wrong secret
echo "Test 2: Wrong secret (expect 401)"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d '{"secret": "wrong", "dummy": "data"}' http://127.0.0.1:9876/submit)
echo "Result: $HTTP_CODE"

# Test 3: Correct secret
echo "Test 3: Correct secret (expect 200)"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d '{"secret": "test_secret", "dummy": "data"}' http://127.0.0.1:9876/submit)
echo "Result: $HTTP_CODE"

kill $MGR_PID
sed -i 's/MANAGER_SECRET = "test_secret"/MANAGER_SECRET = ""/' AE_RenderManager.py
wait $MGR_PID 2>/dev/null || true
