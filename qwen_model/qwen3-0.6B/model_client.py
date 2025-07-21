import requests
import sys

# ===================== 用户配置区域 =====================
# 设置是否开启思考模式（True为开启，False为关闭）
ENABLE_THINKING = False

# 设置服务端地址
SERVER_URL = "http://localhost:5000"

# 设置要提问的内容
QUESTION = "别人问我：'你觉得我这个人怎么样'，我怎么回答？ 不要太官方"
# QUESTION = "明天有雨吗"

# ======================================================

def main():
    try:
        # 发送请求到服务端
        # print(f"正在向服务端发送请求... (思考模式: {'开启' if ENABLE_THINKING else '关闭'})")
        response = requests.post(
            f"{SERVER_URL}/generate",
            json={
                "prompt": QUESTION,
                "enable_thinking": ENABLE_THINKING
            },
            timeout=60
        )
        response.raise_for_status()  # 检查HTTP错误

        # 解析响应
        result = response.json()

        # 输出结果
        if ENABLE_THINKING and result.get('thinking_content'):
            print("思考过程:")
            print(result['thinking_content'])

        print("模型回复:")
        print(result['content'])

    except requests.exceptions.ConnectionError:
        print(f"\n错误: 无法连接到服务端 {SERVER_URL}")
        print("请检查:")
        print("1. 服务端是否正在运行?")
        print("2. 网络连接是否正常?")
        print("3. 防火墙是否阻止了连接?")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print("\n错误: 请求超时，服务端响应时间过长")
        print("可能原因:")
        # 发给ai改改  ok
        print("1. 问题过于复杂")
        print("2. 模型处理时间过长")
        print("3. 网络延迟太高")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"\nHTTP错误: {e.response.status_code}")
        print(f"详细信息: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"\n未知错误: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()