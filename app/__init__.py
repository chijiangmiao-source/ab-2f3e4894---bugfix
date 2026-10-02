"""高危遥控值班系统：选择(selection) — 执行(execution)服务。

包内模块：
  config    运行配置（数据库路径、MAC 密钥、崩溃注入点）
  security  持久记录的 HMAC 完整性保护
  store     SQLite 持久化与启动恢复
  device    被控载荷模拟器（按稳定操作标识幂等）
  service   选择/执行业务规则与并发裁决
  httpapi   HTTP/JSON 接口
"""

__version__ = "1.0.0"
