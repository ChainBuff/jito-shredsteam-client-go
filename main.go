package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb_shredstream "jito-shredstream-client/proto/shredstream"

	bin "github.com/gagliardetto/binary"
)

// 目标地址映射，可以同时监控多个地址
var targetAddresses = map[string]string{
	// "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":  "Token Program",
	// "11111111111111111111111111111111":             "System Program",
	// "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": "Associated Token Account Program",
	"TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM": "pump",
	// "WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh": "bonk",
	// "GVVUi6DaocSEAp8ATnXFAPNF5irCWjCvmPCzoaGAf5eJ": "boop",
}

// 统计信息
var (
	totalEntries   int64
	parsedEntries  int64
	failedEntries  int64
	totalTxs       int64
	matchedTxs     int64
	parseErrors    int64
	reconnectCount int64
	lastSlot       int64
)

func main() {
	addr := "127.0.0.1:19999" // 当前连接地址

	// addr := "84.246.108.44:19999" // 当前连接地址

	fmt.Printf("尝试连接到 ShredStream 服务: %s\n", addr)

	// 启动统计goroutine
	go printStats()

	// 使用重连循环确保稳定连接
	for {
		if err := connectAndSubscribe(addr); err != nil {
			atomic.AddInt64(&reconnectCount, 1)
			fmt.Printf("连接中断，将在5秒后重连... (第%d次重连) 错误: %v\n",
				atomic.LoadInt64(&reconnectCount), err)
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

func connectAndSubscribe(addr string) error {
	// 设置连接选项：增加消息大小限制和超时设置
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1000 * 1024 * 1024)), // 100MB
		grpc.WithTimeout(30 * time.Second),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return fmt.Errorf("Failed to connect to gRPC server: %w", err)
	}
	defer conn.Close()

	// 创建 ShredStream 客户端
	client := pb_shredstream.NewShredstreamProxyClient(conn)

	fmt.Println("成功连接到服务器，开始订阅 Entries...")

	// 创建流式请求
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.SubscribeEntries(ctx, &pb_shredstream.SubscribeEntriesRequest{})
	if err != nil {
		return fmt.Errorf("Failed to subscribe to stream: %w", err)
	}

	fmt.Println("已成功订阅，等待接收数据...")

	// 输出监控的地址列表
	fmt.Println("正在监控以下地址的交易:")
	for addr, desc := range targetAddresses {
		fmt.Printf("- %s (%s)\n", addr, desc)
	}

	pool, err := ants.NewPool(10000, ants.WithPanicHandler(func(err interface{}) {
		log.Printf("panic in pool: %v", err)
		atomic.AddInt64(&parseErrors, 1)
	}), ants.WithPreAlloc(true), ants.WithNonblocking(true))
	if err != nil {
		return fmt.Errorf("创建协程池失败: %w", err)
	}
	defer pool.Release()

	// 接收和处理交易
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("Stream closed normally")
			return nil
		}

		// 复制响应以避免竞态条件
		entryCopy := &pb_shredstream.Entry{
			Slot:    resp.GetSlot(),
			Entries: make([]byte, len(resp.GetEntries())),
		}
		copy(entryCopy.Entries, resp.GetEntries())

		pool.Submit(func() {
			if err := processEntry(entryCopy); err != nil {
				atomic.AddInt64(&failedEntries, 1)
				failed := atomic.LoadInt64(&failedEntries)
				total := atomic.LoadInt64(&totalEntries)
				if failed%100 == 0 || (total > 0 && float64(failed)/float64(total) > 0.1) {
					fmt.Printf("警告: 已有 %d 个 Entry 解析失败 (失败率: %.2f%%)\n",
						failed, float64(failed)/float64(total)*100)
				}
			} else {
				atomic.AddInt64(&parsedEntries, 1)
			}
		})

		atomic.AddInt64(&totalEntries, 1)
		// 检查 slot 连续性
		currentSlot := int64(resp.GetSlot())
		lastSlotValue := atomic.LoadInt64(&lastSlot)
		if lastSlotValue > 0 && currentSlot > lastSlotValue+10 {
			fmt.Printf("警告: 检测到 slot 跳跃，可能丢失数据。上一个: %d, 当前: %d\n",
				lastSlotValue, currentSlot)
		}
		atomic.StoreInt64(&lastSlot, currentSlot)
	}
}

func printStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		total := atomic.LoadInt64(&totalEntries)
		parsed := atomic.LoadInt64(&parsedEntries)
		txs := atomic.LoadInt64(&totalTxs)
		matched := atomic.LoadInt64(&matchedTxs)
		errors := atomic.LoadInt64(&parseErrors)
		reconnects := atomic.LoadInt64(&reconnectCount)
		currentSlot := atomic.LoadInt64(&lastSlot)

		if total > 0 {
			successRate := float64(parsed) / float64(total) * 100
			matchRate := float64(matched) / float64(txs) * 100

			fmt.Printf("\n=== 统计信息 (Slot: %d) ===\n", currentSlot)
			fmt.Printf("总 Entries: %d, 成功解析: %d (%.2f%%), 失败: %d\n",
				total, parsed, successRate, errors)
			fmt.Printf("总交易数: %d, 匹配交易: %d (%.4f%%), 解析错误: %d\n",
				txs, matched, matchRate, errors)
			fmt.Printf("重连次数: %d\n", reconnects)
		}
	}
}

func processEntry(entry *pb_shredstream.Entry) error {

	// 获取 Entry 中的交易数据
	entriesData := entry.GetEntries()

	// 基本数据验证
	if len(entriesData) < 48 {
		return fmt.Errorf("数据太小，可能不包含有效 Entry: %d bytes", len(entriesData))
	}

	var solanaEntry *SolanaEntry
	var err error

	solanaEntry, err = parseJitoEntry(entriesData)
	if err != nil {
		fmt.Println("标准解析失败", err)
		return fmt.Errorf("所有解析方法都失败: %w", err)
	}

	// 统计交易数量
	atomic.AddInt64(&totalTxs, int64(len(solanaEntry.Transactions)))

	// 处理每个交易
	for i, tx := range solanaEntry.Transactions {

		// if len(tx.Message.AccountKeys) != 17 {
		// 	continue
		// }

		addressesFound := containsAddresses(&tx.Message, &tx)
		if len(addressesFound) > 0 {
			atomic.AddInt64(&matchedTxs, 1)

			// 输出找到的地址
			fmt.Printf("\n%s找到包含目标地址的交易: slot=%d, tx=%d/%d, https://solscan.io/tx/%s\n",
				time.Now().Format(time.StampMicro),
				entry.GetSlot(),
				i+1,
				len(solanaEntry.Transactions),
				tx.Signatures[0].String())

			for addr, desc := range addressesFound {
				fmt.Printf("- 包含地址: %s (%s)\n", addr, desc)
			}

			// 打印交易的更多详细信息
			printTransactionDetails(&tx)
			fmt.Println()
		}
	}

	return nil
}

// 标准 Solana Entry 解析
func parseStandardEntry(data []byte) (*SolanaEntry, error) {
	decoder := bin.NewBinDecoder(data)

	// 读取 num_hashes
	numHashes, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return nil, fmt.Errorf("读取 num_hashes 失败: %w", err)
	}

	// 合理性检查
	if numHashes > 1000000 {
		return nil, fmt.Errorf("num_hashes 值不合理: %d", numHashes)
	}

	// 读取 hash (32 bytes)
	hashBytes := make([]byte, 32)
	if _, err := decoder.Read(hashBytes); err != nil {
		return nil, fmt.Errorf("读取 hash 失败: %w", err)
	}

	var hash solana.Hash
	copy(hash[:], hashBytes)

	// 读取交易数量
	numTxns, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return nil, fmt.Errorf("读取交易数量失败: %w", err)
	}

	// 合理性检查
	if numTxns > 10000 {
		return nil, fmt.Errorf("交易数量不合理: %d", numTxns)
	}

	// 检查剩余字节是否足够
	estimatedBytesNeeded := numTxns * 100 // 假设每个交易至少100字节
	if estimatedBytesNeeded > uint64(decoder.Remaining()) {
		return nil, fmt.Errorf("剩余字节不足以读取 %d 个交易", numTxns)
	}

	// 读取交易
	transactions := make([]solana.Transaction, 0, numTxns)
	for i := uint64(0); i < numTxns; i++ {
		var tx solana.Transaction
		if err := tx.UnmarshalWithDecoder(decoder); err != nil {
			// 只有第一个交易失败才返回错误，否则返回已解析的交易
			if i == 0 {
				return nil, fmt.Errorf("第一个交易解析失败: %w", err)
			}
			break
		}
		transactions = append(transactions, tx)
	}

	return &SolanaEntry{
		NumHashes:    numHashes,
		Hash:         hash,
		Transactions: transactions,
	}, nil
}

// Jito 格式 Entry 解析（带头部）
func parseJitoEntry(data []byte) (*SolanaEntry, error) {
	decoder := bin.NewBinDecoder(data)

	// 检查是否有足够的字节
	if decoder.Remaining() < 16 {
		return nil, fmt.Errorf("数据太短，无法包含头部+NumHashes")
	}

	// 跳过头部信息（8字节）
	if _, err := decoder.ReadUint64(bin.LE); err != nil {
		return nil, fmt.Errorf("跳过头部失败: %w", err)
	}

	// 使用标准解析剩余部分
	return parseStandardEntry(data[8:])
}

func printTransactionDetails(tx *solana.Transaction) {
	// 打印交易的基本信息
	fmt.Printf("交易详情:\n")
	fmt.Printf("- 签名: https://solscan.io/tx/%s\n", tx.Signatures[0].String())

	// 打印账户信息
	fmt.Printf("- 重要账户列表:\n")

	for i, key := range tx.Message.AccountKeys {
		// if i >= maxAccountsToShow && len(tx.Message.AccountKeys) > maxAccountsToShow {
		// 	remainingAccounts := len(tx.Message.AccountKeys) - maxAccountsToShow
		// 	fmt.Printf("  ... 还有 %d 个账户未显示\n", remainingAccounts)
		// 	break
		// }

		// 检查账户权限
		isWritable := "只读"
		isSigner := "非签名者"

		// 根据Solana交易消息格式确定权限
		numRequiredSignatures := int(tx.Message.Header.NumRequiredSignatures)
		numReadonlySignedAccounts := int(tx.Message.Header.NumReadonlySignedAccounts)
		numReadonlyUnsignedAccounts := int(tx.Message.Header.NumReadonlyUnsignedAccounts)

		// 判断是否是签名者
		if i < numRequiredSignatures {
			isSigner = "签名者"

			// 判断签名者是否可写
			if i < numRequiredSignatures-numReadonlySignedAccounts {
				isWritable = "可写"
			}
		} else {
			// 判断非签名者是否可写
			totalReadonlyAccounts := numReadonlySignedAccounts + numReadonlyUnsignedAccounts
			if i < len(tx.Message.AccountKeys)-totalReadonlyAccounts {
				isWritable = "可写"
			}
		}

		isPayer := ""
		if i == 0 {
			isPayer = " (手续费支付者)"
		}

		fmt.Printf("  %d: %s [%s, %s]%s\n", i, key.String(), isWritable, isSigner, isPayer)
	}

	// 打印指令信息
	fmt.Printf("- 指令列表 (共%d个):\n", len(tx.Message.Instructions))

	// 只打印前3个指令，如果有更多指令，显示省略提示
	// maxInstructionsToShow := 3
	for i, inst := range tx.Message.Instructions {
		// if i >= maxInstructionsToShow && len(tx.Message.Instructions) > maxInstructionsToShow {
		// 	remainingInsts := len(tx.Message.Instructions) - maxInstructionsToShow
		// 	fmt.Printf("  ... 还有 %d 个指令未显示\n", remainingInsts)
		// 	break
		// }

		programID := tx.Message.AccountKeys[inst.ProgramIDIndex]
		programName := getProgramName(programID.String())

		fmt.Printf("  指令 %d: 程序 %s (%s)\n", i, programID.String(), programName)

		// 尝试解释指令
		if len(inst.Data) > 0 {
			decodeInstructionData(programID.String(), inst.Data)
		}
	}

	// 打印元数据
	fmt.Printf("- 最近的区块哈希: %s\n", tx.Message.RecentBlockhash.String())

	// 估算交易大小
	txSize := estimateTransactionSize(tx)
	fmt.Printf("- 估计大小: %d 字节\n", txSize)
}

// 根据程序ID返回常见的程序名称
func getProgramName(programID string) string {
	knownPrograms := map[string]string{
		"11111111111111111111111111111111":             "System Program",
		"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":  "Token Program",
		"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": "Associated Token Account Program",
		"metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s":  "Metaplex Token Metadata",
		"mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68":  "Mango v3",
		"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin": "Serum DEX v3",
		"DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1": "Solend Program",
		"namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX":  "Solana Name Service",
		"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4":  "Jupiter Aggregator",
		"ComputeBudget111111111111111111111111111111":  "Compute Budget Program",
		"So11111111111111111111111111111111111111112":  "Wrapped SOL",
	}

	if name, exists := knownPrograms[programID]; exists {
		return name
	}

	return "未知程序"
}

// 尝试解码指令数据
func decodeInstructionData(programID string, data []byte) {
	// 系统程序指令解码
	if programID == "11111111111111111111111111111111" && len(data) > 4 {
		// 系统程序的前4个字节是指令类型
		instructionType := binary.LittleEndian.Uint32(data[:4])
		systemInstructions := map[uint32]string{
			0: "创建账户",
			1: "分配空间",
			2: "分配空间并赋予所有权",
			3: "转账 SOL",
			4: "创建账户并转账 SOL",
			5: "升级可加载程序",
			6: "创建不可编写账户",
			7: "转移所有权",
			8: "创建不可编写账户并转移所有权",
		}

		if name, exists := systemInstructions[instructionType]; exists {
			fmt.Printf("    系统程序指令: %s\n", name)

			// 如果是转账，尝试解析金额
			if instructionType == 3 && len(data) >= 12 {
				amount := binary.LittleEndian.Uint64(data[4:12])
				fmt.Printf("    转账金额: %d lamports (%.9f SOL)\n", amount, float64(amount)/1e9)
			}
		}
	}

	// Token程序指令解码
	if programID == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" && len(data) > 0 {
		instructionType := data[0]
		tokenInstructions := map[byte]string{
			0:  "初始化代币铸造",
			1:  "创建代币账户",
			2:  "铸造代币",
			3:  "销毁代币",
			4:  "转移代币",
			5:  "授权代币操作",
			6:  "撤销代币操作授权",
			7:  "设置代币账户所有者",
			8:  "铸币授权",
			9:  "设置代币账户关闭授权",
			10: "关闭代币账户",
			11: "冻结代币账户",
			12: "解冻代币账户",
		}

		if name, exists := tokenInstructions[instructionType]; exists {
			fmt.Printf("    代币程序指令: %s\n", name)

			// 如果是转移代币，尝试解析金额
			if instructionType == 4 && len(data) >= 9 {
				amount := binary.LittleEndian.Uint64(data[1:9])
				fmt.Printf("    转移数量: %d 代币单位\n", amount)
			}
		}
	}
}

// 估算交易大小
func estimateTransactionSize(tx *solana.Transaction) int {
	// 签名: 每个64字节
	sigSize := len(tx.Signatures) * 64

	// 消息头: 3字节
	headerSize := 3

	// 最近的区块哈希: 32字节
	recentBlockhashSize := 32

	// 账户数量: 可变，但通常每个32字节
	accountsSize := len(tx.Message.AccountKeys) * 32

	// 指令: 可变
	instructionsSize := 0
	for _, inst := range tx.Message.Instructions {
		// 程序ID索引: 1字节
		// 账户数量: 1字节
		// 账户索引: 每个1字节
		// 数据长度: 可变
		// 数据: 可变
		instructionsSize += 2 + len(inst.Accounts) + len(inst.Data)
	}

	// 其他元数据和填充: ~10字节
	otherSize := 10

	return sigSize + headerSize + recentBlockhashSize + accountsSize + instructionsSize + otherSize
}

// 检查消息是否包含目标地址，返回找到的地址映射
func containsAddresses(message *solana.Message, tx *solana.Transaction) map[string]string {
	foundAddresses := make(map[string]string)

	for i, key := range message.AccountKeys {
		keyStr := key.String()
		if desc, exists := targetAddresses[keyStr]; exists {
			fmt.Println("len(message.AccountKeys)", len(message.AccountKeys))
			fmt.Printf("https://solscan.io/tx/%s\n", tx.Signatures[0].String())
			// if desc == "bonk" {
			// 	// 检查账户数量是否符合预期
			// 	if len(message.AccountKeys) != 17 {
			// 		// 记录不匹配的情况以便调试
			// 		if len(foundAddresses) == 0 {
			// 			fmt.Printf("调试: bonk地址存在但账户数量不匹配，实际: %d, 预期: 17\n", len(message.AccountKeys))
			// 			printTransactionDetails(tx)
			// 		}
			// 		return foundAddresses
			// 	}

			// 	mint := base58.Encode(message.AccountKeys[1].Bytes())
			// 	if !strings.HasSuffix(mint, "bonk") {
			// 		return foundAddresses
			// 	}
			// }

			foundAddresses[keyStr] = desc
			fmt.Printf("调试: 在账户索引 %d 找到目标地址 %s (%s)\n", i, keyStr, desc)
		}
	}

	return foundAddresses
}

// 更新Entry结构体，使用标准Solana格式
type SolanaEntry struct {
	NumHashes    uint64               `json:"num_hashes"`
	Hash         solana.Hash          `json:"hash"`
	Transactions []solana.Transaction `json:"transactions"`
}

// 标准解析方法 (无头部)
func (en *SolanaEntry) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	// 读取 num_hashes
	if en.NumHashes, err = decoder.ReadUint64(bin.LE); err != nil {
		return fmt.Errorf("failed to read number of hashes at position %d: %w", decoder.Position(), err)
	}

	// 读取 hash (32 bytes)
	hashBytes := make([]byte, 32)
	bytesRead, err := decoder.Read(hashBytes)
	if err != nil {
		return fmt.Errorf("failed to read hash at position %d: %w", decoder.Position(), err)
	}
	if bytesRead != 32 {
		return fmt.Errorf("expected to read 32 bytes for hash, but got %d", bytesRead)
	}

	copy(en.Hash[:], hashBytes)

	// 读取交易数量
	numTxns, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read number of transactions at position %d: %w", decoder.Position(), err)
	}

	// 安全检查：确保交易数量合理
	if numTxns > 10000 {
		return fmt.Errorf("unreasonable number of transactions: %d", numTxns)
	}

	// 额外安全检查：确保有足够的字节来读取声明的交易数量
	estimatedBytesNeeded := numTxns * 100 // 假设每个交易至少100字节
	if estimatedBytesNeeded > uint64(decoder.Remaining()) {
		return fmt.Errorf("not enough bytes to read %d transactions, need ~%d bytes but only %d remaining",
			numTxns,
			estimatedBytesNeeded,
			decoder.Remaining())
	}

	// 读取交易列表
	en.Transactions = make([]solana.Transaction, 0, numTxns)
	for i := uint64(0); i < numTxns; i++ {
		txStartPos := decoder.Position()
		var tx solana.Transaction
		if err = tx.UnmarshalWithDecoder(decoder); err != nil {
			return fmt.Errorf("failed to decode transaction %d/%d at position %d: %w",
				i+1, numTxns, txStartPos, err)
		}
		en.Transactions = append(en.Transactions, tx)
	}

	return nil
}

// 大端序解析方法
func (en *SolanaEntry) UnmarshalWithBigEndian(decoder *bin.Decoder) (err error) {
	// 读取 num_hashes
	if en.NumHashes, err = decoder.ReadUint64(bin.BE); err != nil {
		return fmt.Errorf("failed to read number of hashes: %w", err)
	}

	// 对NumHashes进行合理性检查
	if en.NumHashes > 1000000 {
		return fmt.Errorf("num_hashes value unreasonable: %d", en.NumHashes)
	}

	// 读取 hash (32 bytes)
	hashBytes := make([]byte, 32)
	if _, err = decoder.Read(hashBytes); err != nil {
		return fmt.Errorf("failed to read hash: %w", err)
	}
	copy(en.Hash[:], hashBytes)

	// 读取交易数量
	var numTxns uint64
	if numTxns, err = decoder.ReadUint64(bin.BE); err != nil {
		return fmt.Errorf("failed to read number of transactions: %w", err)
	}

	// 检查合理性
	if numTxns > 10000 {
		return fmt.Errorf("transaction count unreasonable: %d", numTxns)
	}

	// 安全检查
	estimatedBytesNeeded := numTxns * 100
	if estimatedBytesNeeded > uint64(decoder.Remaining()) {
		return fmt.Errorf("not enough bytes for %d transactions", numTxns)
	}

	// 读取交易
	en.Transactions = make([]solana.Transaction, 0, numTxns)
	for i := uint64(0); i < numTxns; i++ {
		var tx solana.Transaction
		if err = tx.UnmarshalWithDecoder(decoder); err != nil {
			// 交易解析失败，但可能只是第一个交易有问题
			// 如果至少解析出一些交易，可以继续
			if i > 0 {
				break
			}
			return fmt.Errorf("failed to decode first transaction: %w", err)
		}
		en.Transactions = append(en.Transactions, tx)
	}

	return nil
}
