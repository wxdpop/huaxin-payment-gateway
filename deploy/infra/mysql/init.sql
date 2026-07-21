-- ============================================================================
-- 华鑫融汇聚合支付网关 - MySQL 数据库初始化脚本
-- ============================================================================
-- 说明:
--   本脚本由 docker-compose 中 mysql 容器启动时自动执行
--   (挂载到 /docker-entrypoint-initdb.d/init.sql)
--   仅在数据目录为空(首次启动)时执行一次,后续重启不会重复执行
--   包含内容:
--     1. 8 张核心业务表结构(商户/渠道/订单/支付记录/账户/流水/退款/幂等)
--     2. 关键唯一约束与索引(防重复下单/防重复入账/幂等控制)
--     3. 示例数据(3 商户 / 3 渠道 / 3 账户),便于联调测试
-- 学习要点:
--   - BIGINT AUTO_INCREMENT: MySQL 自增主键(对应 PostgreSQL 的 BIGSERIAL)
--   - DATETIME(3): 毫秒精度时间戳(MySQL DATETIME 默认秒级,金融场景需毫秒精度)
--   - JSON: MySQL 8.0 原生 JSON 类型(对应 PostgreSQL 的 JSONB),支持索引与函数查询
--   - DECIMAL(18,2): 精确小数,金融金额绝对禁止用 FLOAT/DOUBLE(浮点误差)
--   - utf8mb4 字符集: 支持 emoji 与全角字符(MySQL utf8 是 3 字节,emoji 4 字节会报错)
--   - KEY 而非 INDEX: MySQL 中 KEY/INDEX 同义,但 PRIMARY KEY 语法用 KEY
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 表1: merchants 商户表
-- 用途: 存储接入支付网关的商户信息(商户号/名称/状态/签名密钥/费率配置)
-- ----------------------------------------------------------------------------
CREATE TABLE merchants (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,           -- 主键: 无符号自增大整数(MySQL 用 UNSIGNED 避免负值)
    merchant_no     VARCHAR(32) NOT NULL,                              -- 商户号: 全局唯一,业务侧标识(如 M001)
    name            VARCHAR(128) NOT NULL,                            -- 商户名称
    status          TINYINT NOT NULL DEFAULT 1,                       -- 状态: 1启用 0停用(TINYINT 节省空间)
    private_key     VARCHAR(256),                                     -- 签名私钥: 用于验签商户请求(生产应加密存储)
    rate_config     JSON,                                             -- 费率配置: JSON 格式,支持按渠道/金额分段费率
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), -- 创建时间: 默认当前时间(3=毫秒精度)
    updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), -- 更新时间: 业务层更新时同步刷新
    PRIMARY KEY (id),
    UNIQUE KEY uk_merchant_no (merchant_no)                           -- 商户号唯一约束
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='商户表: 存储接入网关的商户基本信息与费率配置';
-- 学习要点: MySQL 表级 COMMENT 在 INFORMATION_SCHEMA.TABLES 中可查
--   InnoDB 引擎支持事务与行级锁(必须),MyISAM 不支持事务(禁用于金融场景)


-- ----------------------------------------------------------------------------
-- 表2: payment_channels 支付渠道表
-- 用途: 存储第三方支付通道(微信/支付宝/银联)的配置信息
--       应用启动时加载到内存,路由模块根据此表选择通道
-- ----------------------------------------------------------------------------
CREATE TABLE payment_channels (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    channel_code    VARCHAR(32) NOT NULL,                             -- 渠道编码: wechat/alipay/unionpay,全局唯一
    channel_name    VARCHAR(64),                                      -- 渠道名称(中文展示用)
    status          TINYINT NOT NULL DEFAULT 1,                       -- 状态: 1启用 0停用(停用后路由不再选择)
    config          JSON,                                             -- 通道配置: 应用ID/证书路径/回调地址等(JSON)
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_channel_code (channel_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='支付渠道表: 微信/支付宝/银联等第三方通道配置';


-- ----------------------------------------------------------------------------
-- 表3: orders 订单表 (核心表)
-- 用途: 存储平台订单,记录下单/支付/退款全生命周期
-- 关键设计:
--   ① uk_merchant_out_trade_no: 商户ID+商户订单号联合唯一,防止同一商户重复下单
--   ② idx_orders_channel_order_no: 普通索引,渠道回调时按渠道订单号快速定位
--   ③ idx_orders_status_created: 状态+创建时间复合索引,便于运营按状态查询与对账
-- ----------------------------------------------------------------------------
CREATE TABLE orders (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_no        VARCHAR(32) NOT NULL,                              -- 平台订单号: 全局唯一,网关生成(如 PG20250714001)
    merchant_id     BIGINT UNSIGNED NOT NULL,                         -- 商户ID: 关联 merchants 表(应用层校验,无物理外键)
    channel_code    VARCHAR(32),                                      -- 渠道编码: 路由后选定(下单时可能为空,路由后填充)
    channel_order_no VARCHAR(64),                                     -- 渠道侧订单号: 第三方返回的预支付订单号
    out_trade_no    VARCHAR(64) NOT NULL,                             -- 商户订单号: 商户侧自有订单号
    subject         VARCHAR(256),                                     -- 订单标题/商品描述
    amount          DECIMAL(18,2) NOT NULL,                           -- 金额: 单位元,2位小数(如 99.50),金融禁用浮点
    status          TINYINT NOT NULL DEFAULT 0,                       -- 状态: 0待支付 1支付中 2已支付 3已退款 4已关闭
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    paid_at         DATETIME(3) NULL,                                 -- 支付完成时间(支付成功时回填,可空)
    updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_no (order_no),                                -- 平台订单号唯一
    -- 关键约束: 商户ID + 商户订单号 联合唯一,从 DB 层防止重复下单(幂等)
    UNIQUE KEY uk_merchant_out_trade_no (merchant_id, out_trade_no),
    KEY idx_orders_channel_order_no (channel_order_no),               -- 普通索引(用于按渠道订单号查找)
    KEY idx_orders_status_created (status, created_at)                -- 复合索引(按状态分页查询)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='订单表(核心): 记录下单到支付完成的全生命周期';
-- 学习要点: MySQL 不强制外键约束(性能考虑),应用层保证关联完整性
--   避免物理外键: 分布式场景下分库分表不便,且级联锁影响并发性能


-- ----------------------------------------------------------------------------
-- 表4: payment_records 支付记录表
-- 用途: 记录每次支付请求与回调的详细信息,是回调幂等的核心保障
-- 关键设计:
--   uk_channel_order: 渠道编码+渠道订单号联合唯一,DB 层兜底防重复入账
--   callback_raw: 保存原始回调报文,便于排查与对账
-- ----------------------------------------------------------------------------
CREATE TABLE payment_records (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id        BIGINT UNSIGNED NOT NULL,                          -- 订单ID: 关联 orders 表
    channel_code    VARCHAR(32) NOT NULL,                             -- 渠道编码: 标识来自哪个支付通道
    channel_order_no VARCHAR(64) NOT NULL,                            -- 渠道订单号: 第三方返回的唯一订单号
    channel_trade_no VARCHAR(64),                                     -- 渠道交易号: 第三方支付流水号(支付成功后才有)
    amount          DECIMAL(18,2) NOT NULL,                          -- 支付金额(元)
    status          TINYINT NOT NULL DEFAULT 0,                      -- 状态: 0待支付 1成功 2失败
    callback_raw    TEXT,                                             -- 回调原始报文: 完整保存第三方回调 JSON/XML(排查用)
    callback_at     DATETIME(3) NULL,                                 -- 回调到达时间
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    -- 关键约束: 渠道编码 + 渠道订单号 联合唯一,DB 层防止重复入账(金融幂等核心)
    UNIQUE KEY uk_channel_order (channel_code, channel_order_no),
    KEY idx_payment_records_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='支付记录表: 记录支付请求与回调详情,渠道+渠道订单号唯一防重复入账';


-- ----------------------------------------------------------------------------
-- 表5: accounts 资金账户表 (核心表)
-- 用途: 存储商户资金账户余额,是资金变更的临界区(需分布式锁保护)
-- 关键设计:
--   ① merchant_id UNIQUE: 一个商户对应一个账户(1:1)
--   ② version 乐观锁: 并发更新时 CAS 校验,与 ZK 锁形成双重保障
--   ③ frozen_amount: 退款时先冻结金额,退款成功再扣减,失败则解冻
-- ----------------------------------------------------------------------------
CREATE TABLE accounts (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    merchant_id     BIGINT UNSIGNED NOT NULL,                         -- 商户ID: 唯一约束(一商户一账户)
    balance         DECIMAL(18,2) NOT NULL DEFAULT 0.00,             -- 可用余额: 可提现/消费的金额
    frozen_amount   DECIMAL(18,2) NOT NULL DEFAULT 0.00,             -- 冻结金额: 退款预处理时冻结(退款成功扣减,失败解冻)
    version         BIGINT NOT NULL DEFAULT 0,                        -- 乐观锁版本号: 每次更新+1,CAS 校验防并发覆盖
    updated_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_account_merchant (merchant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='资金账户表(核心): 商户余额,乐观锁 version 字段防并发更新';
-- 学习要点: MySQL 的 DECIMAL 默认值需明确指定类型(0.00 而非 0),避免精度问题


-- ----------------------------------------------------------------------------
-- 表6: account_transactions 账户流水表
-- 用途: 记录账户每一笔资金变动(收入/支出/冻结/解冻),用于对账与审计
-- 关键设计:
--   uk_account_tx_biz_no: 业务单号唯一,防止重复记账(幂等)
--   balance_after: 变更后余额快照,便于追溯任意时刻余额
-- ----------------------------------------------------------------------------
CREATE TABLE account_transactions (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    account_id      BIGINT UNSIGNED NOT NULL,                         -- 账户ID: 关联 accounts 表
    order_id        BIGINT UNSIGNED NULL,                            -- 关联订单ID(可空,如充值无对应订单)
    tx_type         TINYINT NOT NULL,                                -- 交易类型: 1收入 2支出 3冻结 4解冻
    amount          DECIMAL(18,2) NOT NULL,                          -- 变动金额(始终为正数,类型区分收/支)
    balance_after   DECIMAL(18,2) NOT NULL,                          -- 变更后余额快照(对账与审计关键)
    biz_no          VARCHAR(64) NOT NULL,                            -- 业务单号: 关联业务(如支付单号/退款单号)
    remark          VARCHAR(256),                                    -- 备注(如"微信支付入账"/"退款冻结")
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    -- 关键约束: 业务单号唯一,防止重复记账(DB 层幂等兜底)
    UNIQUE KEY uk_account_tx_biz_no (biz_no),
    KEY idx_account_tx_account (account_id, created_at DESC)         -- 复合索引(按账户查询流水)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='账户流水表: 记录每笔资金变动,业务单号唯一防重复记账';


-- ----------------------------------------------------------------------------
-- 表7: refund_records 退款记录表
-- 用途: 记录退款申请与处理结果,退款涉及资金回退与账户变动
-- ----------------------------------------------------------------------------
CREATE TABLE refund_records (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    refund_no       VARCHAR(32) NOT NULL,                             -- 退款单号: 全局唯一,网关生成
    order_id        BIGINT UNSIGNED NOT NULL,                         -- 原订单ID: 关联(退款必须基于已支付订单)
    merchant_id     BIGINT UNSIGNED NOT NULL,                        -- 商户ID: 冗余字段,便于按商户查询退款
    amount          DECIMAL(18,2) NOT NULL,                           -- 退款金额(元)
    status          TINYINT NOT NULL DEFAULT 0,                      -- 状态: 0待退款 1已退款 2失败
    channel_refund_no VARCHAR(64),                                   -- 渠道退款单号: 第三方返回的退款流水号
    reason          VARCHAR(256),                                    -- 退款原因(如"用户申请"/"商品缺货")
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    finished_at     DATETIME(3) NULL,                                -- 完成时间(退款成功或失败时回填)
    PRIMARY KEY (id),
    UNIQUE KEY uk_refund_no (refund_no),
    KEY idx_refund_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='退款记录表: 退款申请与处理结果';


-- ----------------------------------------------------------------------------
-- 表8: idempotent_records 幂等记录表 (HTTP 入口幂等)
-- 用途: 实现 HTTP 接口幂等,客户端携带 Idempotency-Key,5分钟内重复请求返回首次结果
-- 工作原理:
--   ① 首次请求: 记录 Key + 响应体 + 过期时间,执行业务
--   ② 重复请求: 命中记录,直接返回首次响应体(不重复执行业务)
--   ③ 过期清理: 定时任务删除 expires_at < NOW() 的记录
-- 学习要点: 本工程 HTTP 幂等基于 Redis 实现,此表为可选的 DB 兜底方案
-- ----------------------------------------------------------------------------
CREATE TABLE idempotent_records (
    idempotency_key VARCHAR(64) NOT NULL,                            -- 幂等键: 客户端请求头 Idempotency-Key 传入(主键)
    merchant_id     BIGINT UNSIGNED NOT NULL,                         -- 商户ID: 标识请求来源
    request_path    VARCHAR(256),                                    -- 请求路径(记录接口,便于排查)
    response_body   TEXT,                                            -- 首次响应体: 重复请求时原样返回
    status_code     INT,                                             -- 首次响应状态码(如 200/400)
    expires_at      DATETIME(3) NOT NULL,                            -- 过期时间(默认5分钟后,过期可被重新执行)
    created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (idempotency_key),
    KEY idx_idempotent_expires (expires_at)                          -- 过期时间索引(定时清理扫描)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='幂等记录表: HTTP 入口幂等,5分钟内同 Key 返回首次结果';


-- ============================================================================
-- 示例数据初始化
-- ============================================================================
-- 说明: 以下数据用于学习联调,生产环境应通过运营后台维护
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 示例商户数据 (3 个商户,不同费率配置)
-- rate_config 字段说明: JSON 格式,按渠道配置费率(小数,如 0.006 = 千分之六)
-- 学习要点: MySQL 8.0 JSON 字段插入需用单引号包裹 JSON 字符串
-- ----------------------------------------------------------------------------
INSERT INTO merchants (merchant_no, name, status, private_key, rate_config) VALUES
('M001', '测试商户A', 1, 'private_key_m001_demo', '{"wechat": 0.006, "alipay": 0.0038, "unionpay": 0.005}'),
('M002', '测试商户B', 1, 'private_key_m002_demo', '{"wechat": 0.004, "alipay": 0.0028, "unionpay": 0.004}'),
('M003', '测试商户C', 0, 'private_key_m003_demo', '{"wechat": 0.006, "alipay": 0.0038, "unionpay": 0.005}');


-- ----------------------------------------------------------------------------
-- 示例支付渠道数据 (3 个渠道: 微信/支付宝/银联)
-- config 字段说明: JSON 格式,包含 appId/mchId/回调URL等(学习工程使用 Mock 配置)
-- ----------------------------------------------------------------------------
INSERT INTO payment_channels (channel_code, channel_name, status, config) VALUES
('wechat',  '微信支付', 1, '{"appId": "wx_demo_appid", "mchId": "1900000109", "callbackUrl": "http://localhost:5000/api/v1/callbacks/wechat", "signType": "HMAC-SHA256"}'),
('alipay',  '支付宝',   1, '{"appId": "2021000_demo", "merchantPrivateKey": "alipay_private_key_demo", "callbackUrl": "http://localhost:5000/api/v1/callbacks/alipay", "signType": "RSA2"}'),
('unionpay','银联',     1, '{"merId": "777290000_demo", "callbackUrl": "http://localhost:5000/api/v1/callbacks/unionpay", "signType": "SHA256WithRSA"}');


-- ----------------------------------------------------------------------------
-- 示例账户数据 (3 个账户,每商户初始余额 10000.00 元)
-- 关系: merchants.id 与 accounts.merchant_id 一一对应
-- ----------------------------------------------------------------------------
INSERT INTO accounts (merchant_id, balance, frozen_amount, version) VALUES
(1, 10000.00, 0.00, 0),
(2, 10000.00, 0.00, 0),
(3, 10000.00, 0.00, 0);


-- ============================================================================
-- 初始化完成
-- ============================================================================
-- 验证查询(可在 mysql 客户端执行):
--   USE payment_gateway;
--   SELECT * FROM merchants;          -- 应有 3 条商户记录
--   SELECT * FROM payment_channels;   -- 应有 3 条渠道记录
--   SELECT * FROM accounts;           -- 应有 3 条账户记录,余额均为 10000.00
-- ============================================================================
