// Command orderfence 运行执行层场景并把结果输出成 Markdown。
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/peter123023/orderfence/internal/report"
	"github.com/peter123023/orderfence/internal/scenarios"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	switch args[0] {
	case "list":
		for _, s := range scenarios.All() {
			fmt.Printf("%-14s %s\n", s.Name, s.Desc)
		}
	case "run":
		run(args[1:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", args[0])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Print(`orderfence — 订单执行层的契约测试台

用法:
  orderfence list                       列出全部场景
  orderfence run [场景名...] [-out 文件] 运行场景，默认全部

示例:
  orderfence run
  orderfence run burst ackloss
  orderfence run all -out report.md
`)
}

func run(args []string) {
	out := ""
	var names []string
	for i := 0; i < len(args); i++ {
		if args[i] == "-out" && i+1 < len(args) {
			out = args[i+1]
			i++
			continue
		}
		names = append(names, args[i])
	}
	if len(names) == 0 || names[0] == "all" {
		names = nil
		for _, s := range scenarios.All() {
			names = append(names, s.Name)
		}
	}

	rep := &report.Report{
		Title:     "orderfence report",
		Generated: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	for _, name := range names {
		sc, ok := scenarios.Find(name)
		if !ok {
			fmt.Fprintf(os.Stderr, "跳过未知场景: %s\n", name)
			continue
		}
		fmt.Fprintf(os.Stderr, "running %s ...\n", sc.Name)
		start := time.Now()
		sec, err := sc.Run(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s 失败: %v\n", sc.Name, err)
			continue
		}
		fmt.Fprintf(os.Stderr, "  done in %s\n", report.Dur(time.Since(start)))
		rep.Add(sec)
	}

	md := rep.Markdown()
	if out == "" {
		fmt.Print(md)
		return
	}
	if err := os.WriteFile(out, []byte(md), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "写文件失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "报告已写入 %s\n", strings.TrimSpace(out))
}
