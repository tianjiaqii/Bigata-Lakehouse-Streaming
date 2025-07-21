import torch


def check_cuda():
    print("=" * 50)
    print(f"PyTorch版本: {torch.__version__}")
    print(f"CUDA是否可用: {torch.cuda.is_available()}")

    if torch.cuda.is_available():
        print(f"设备数量: {torch.cuda.device_count()}")
        print(f"设备名称: {torch.cuda.get_device_name(0)}")
        print(f"CUDA版本: {torch.version.cuda}")

        # 测试 GPU 计算
        a = torch.randn(1000, 1000).cuda()
        b = torch.randn(1000, 1000).cuda()
        c = torch.matmul(a, b)
        print(f"GPU计算测试: 结果形状 {c.shape}, 平均值 {c.mean().item():.4f}")
    else:
        print("!!! CUDA不可用 !!!")

    print("=" * 50)


if __name__ == "__main__":
    check_cuda()