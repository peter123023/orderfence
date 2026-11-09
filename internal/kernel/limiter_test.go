package kernel

import (
	"context"
	"testing"
	"time"
)

func TestBucketThrottles(t *testing.T) {
	b := NewBucket(50, 1) // 50/s，突发 1
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := b.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// 1 个突发令牌加 4 个 20ms 间隔，理论下限 60ms。
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("限速没有生效，5 次只用了 %s", elapsed)
	}
}

func TestBucketPenaltyLowersRate(t *testing.T) {
	b := NewBucket(100, 1)
	before := b.Rate()
	b.Penalize()
	if b.Rate() >= before {
		t.Fatalf("撞限流后速率应下降: %v -> %v", before, b.Rate())
	}
	if b.Penalties() != 1 {
		t.Fatalf("惩罚计数错误: %d", b.Penalties())
	}
}

func TestBucketRespectsContext(t *testing.T) {
	b := NewBucket(1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := b.Wait(ctx); err != nil {
		t.Fatalf("第一个令牌应当立即拿到: %v", err)
	}
	if err := b.Wait(ctx); err == nil {
		t.Fatal("第二个令牌应当等到 ctx 超时")
	}
}
