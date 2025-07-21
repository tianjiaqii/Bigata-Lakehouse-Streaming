from flask import Flask, request, jsonify
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import sys
import traceback

app = Flask(__name__)

# 加载模型和tokenizer
try:
    print("正在加载模型，这可能需要几分钟...")
    model_name = r"E:\qwen3_own\Qwen3-0.6B"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype="auto",
        device_map="auto"
    )
    print(f"模型加载成功！运行在设备: {model.device}")
    print(f"思考模式标记ID: {tokenizer.convert_tokens_to_ids('</think>')}")
except Exception as e:
    print(f"模型加载失败: {str(e)}")
    traceback.print_exc()
    sys.exit(1)  # 退出程序


@app.route('/generate', methods=['POST'])
def generate_response():
    try:
        # 检查客户端连接
        if not request.is_json:
            return jsonify({"error": "请求必须是JSON格式"}), 400

        data = request.get_json()
        prompt = data.get('prompt', '')
        enable_thinking = data.get('enable_thinking', False)  # 默认为关闭思考模式

        if not prompt:
            return jsonify({"error": "问题不能为空"}), 400

        # 构建模型输入
        messages = [{"role": "user", "content": prompt}]
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=enable_thinking  # 使用客户端传入的思考模式设置
        )

        # 准备模型输入
        model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

        # 生成回复
        with torch.no_grad():
            generated_ids = model.generate(
                **model_inputs,
                max_new_tokens=1024  # 适当增加token数量
            )

        # 解析输出
        output_ids = generated_ids[0][len(model_inputs.input_ids[0]):].tolist()

        # 根据思考模式设置解析输出
        thinking_content = ""
        content = ""

        if enable_thinking:
            # 开启思考模式：尝试分离思考内容和实际回复
            try:
                # 查找思考结束标记 </think> (ID: 151668)
                think_end_id = tokenizer.convert_tokens_to_ids('</think>')
                index = len(output_ids) - output_ids[::-1].index(think_end_id)
                thinking_content = tokenizer.decode(output_ids[:index], skip_special_tokens=True).strip("\n")
                content = tokenizer.decode(output_ids[index:], skip_special_tokens=True).strip("\n")
            except ValueError:
                # 如果找不到结束标记，整个内容作为回复
                content = tokenizer.decode(output_ids, skip_special_tokens=True).strip("\n")
        else:
            # 关闭思考模式：直接解码全部内容
            content = tokenizer.decode(output_ids, skip_special_tokens=True).strip("\n")

        # 打印结果到控制台
        print(f"\n[客户端问题]: {prompt}")
        print(f"[思考模式]: {'开启' if enable_thinking else '关闭'}")
        if enable_thinking and thinking_content:
            print(f"[思考过程]: {thinking_content}")
        print(f"[模型回复]: {content}")

        return jsonify({
            "thinking_content": thinking_content,
            "content": content,
            "enable_thinking": enable_thinking
        })

    except Exception as e:
        # 打印详细的错误信息
        error_trace = traceback.format_exc()
        print(f"\n[错误] 处理请求时发生异常: {str(e)}\n{error_trace}")

        return jsonify({
            "error": "处理请求时发生错误",
            "details": str(e)
        }), 500


@app.route('/health', methods=['GET'])
def health_check():
    """健康检查端点"""
    return jsonify({
        "status": "运行正常",
        "model": "Qwen3-0.6B",
        "device": str(model.device),
        "thinking_token_id": tokenizer.convert_tokens_to_ids('</think>')
    })


@app.route('/thinking_example', methods=['GET'])
def thinking_example():
    """返回思考模式使用示例"""
    example = {
        "description": "思考模式API调用示例",
        "request": {
            "method": "POST",
            "url": "/generate",
            "headers": {"Content-Type": "application/json"},
            "body": {
                "prompt": "请解释量子计算的基本原理",
                "enable_thinking": True
            }
        },
        "response": {
            "thinking_content": "用户要求解释量子计算...这是一个复杂的物理概念...",
            "content": "量子计算利用量子力学原理...",
            "enable_thinking": True
        }
    }
    return jsonify(example)


if __name__ == '__main__':
    try:
        # 添加更详细的启动信息
        print("\n服务启动中...")
        print(f"访问 http://localhost:5000/health 进行健康检查")
        print(f"访问 http://localhost:5000/thinking_example 查看思考模式示例")
        print(f"使用 POST 请求 http://localhost:5000/generate 提交问题")
        print("按 Ctrl+C 停止服务\n")

        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
    except Exception as e:
        print(f"无法启动服务: {str(e)}")
        traceback.print_exc()