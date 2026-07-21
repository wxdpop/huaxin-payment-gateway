# ============================================================================
# 华鑫融汇聚合支付网关 - .NET 8 多阶段构建 Dockerfile
# ============================================================================
# 学习要点 - 为什么要用多阶段构建:
#   ① SDK 镜像(mcr.microsoft.com/dotnet/sdk:8.0)约 1GB,含编译器与 NuGet 缓存
#   ② Runtime 镜像(mcr.microsoft.com/dotnet/aspnet:8.0)约 200MB,仅含运行时
#   ③ 多阶段构建: 第一阶段用 SDK 编译产物,第二阶段仅拷贝产物到 Runtime
#   ④ 最终镜像体积小(约 200MB)、攻击面小、启动快
# 构建与运行:
#   docker build -t payment-gateway:latest -f deploy/Dockerfile .      # 构建镜像
#   docker run -d -p 5000:5000 --name payment-gateway payment-gateway:latest   # 运行容器
# 注意:
#   Dockerfile 位于 deploy/ 目录,但构建上下文(context)为项目根目录
#   因此 COPY 路径相对于项目根目录(如 src/PaymentGateway.Api/...)
# ============================================================================


# ----------------------------------------------------------------------------
# 阶段1: build(构建阶段)
# 使用 SDK 镜像进行 restore 与 publish
# ----------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# ★ 多项目构建: 拷贝所有被引用项目的 csproj (路径与 COPY . . 一致)
# 学习要点:
#   ① 单独拷贝 csproj 可命中层缓存,避免每次代码变更都重新还原 NuGet 包
#   ② Api 引用 Application/Infrastructure/Shared,且 Application/Infrastructure 又引用 Domain
#      必须把全部 5 个项目的 csproj 都 COPY 进来,否则 restore 报"project not found"
#      publish 时报 CS5001 找不到 Main 方法
#   ③ COPY 目标路径用 "src/PaymentGateway.Xxx/" 与源码结构一致(COPY . . 后保持路径不变)
COPY ["src/PaymentGateway.Api/PaymentGateway.Api.csproj", "src/PaymentGateway.Api/"]
COPY ["src/PaymentGateway.Application/PaymentGateway.Application.csproj", "src/PaymentGateway.Application/"]
COPY ["src/PaymentGateway.Infrastructure/PaymentGateway.Infrastructure.csproj", "src/PaymentGateway.Infrastructure/"]
COPY ["src/PaymentGateway.Domain/PaymentGateway.Domain.csproj", "src/PaymentGateway.Domain/"]
COPY ["src/PaymentGateway.Shared/PaymentGateway.Shared.csproj", "src/PaymentGateway.Shared/"]
RUN dotnet restore "src/PaymentGateway.Api/PaymentGateway.Api.csproj"

# 拷贝全部源码(此时 csproj 未变则 restore 层命中缓存,无需重新还原)
COPY . .

# 发布 Release 配置到 /app/publish 目录
# 学习要点:
#   ① --no-restore: 已在上一步 restore,跳过避免重复
#   ② 用完整路径指定入口 csproj,无需切换 WORKDIR
#   ③ dotnet publish 会自动编译被引用项目并拷贝其产物到 /app/publish
RUN dotnet publish "src/PaymentGateway.Api/PaymentGateway.Api.csproj" -c Release -o /app/publish --no-restore


# ----------------------------------------------------------------------------
# 阶段2: final(运行阶段)
# 使用 aspnet runtime 镜像(体积小,仅含运行时)
# ----------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app

# ★ 学习要点: aspnet:8.0 基础镜像(基于 Debian)默认不含 curl/wget
#   健康检查需要 curl 探测 HTTP 端点,故需安装
#   --no-install-recommends: 不安装推荐包,减小镜像体积
#   rm -rf /var/lib/apt/lists/*: 清理 apt 缓存,避免镜像膨胀
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# 从 build 阶段拷贝发布产物到当前镜像
COPY --from=build /app/publish .

# 暴露 5000 端口(应用监听端口)
EXPOSE 5000

# 设置 ASP.NET Core 监听地址为 5000 端口
# 学习要点: 默认监听 5000,需显式设置 ASPNETCORE_URLS 避免使用默认 80
ENV ASPNETCORE_URLS=http://+:5000

# 健康检查: 每 30 秒探测 /health 端点
# 学习要点: 区分 liveness(存活)与 readiness(就绪)探针
#   - liveness: 进程是否存活,失败则重启容器 (本工程用 /health)
#   - readiness: 是否能接流量,失败则从负载均衡摘除 (本工程用 /health/ready)
# 注意: Program.cs 中映射的端点是 /health 与 /health/ready (无 /health/live)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# 设置 ASPNETCORE_ENVIRONMENT=Docker,触发加载 appsettings.Docker.json
# 学习要点: 环境变量决定加载哪个 appsettings.{ENV}.json
#   - Development: 本地开发 (用 appsettings.json + appsettings.Development.json,localhost 连接)
#   - Docker: 容器化部署 (用 appsettings.json + appsettings.Docker.json,容器间服务名连接)
ENV ASPNETCORE_ENVIRONMENT=Docker

# 入口点: 启动应用
ENTRYPOINT ["dotnet", "PaymentGateway.Api.dll"]
