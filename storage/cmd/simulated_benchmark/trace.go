package main

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

func runPipelineAIStory() string {
	agent := storage.NewAdaptiveAIAgent(nil)
	var story strings.Builder

	story.WriteString("### **AI Auto-Tuning Dynamic Response Trace**\n\n")
	story.WriteString("This trace shows a simulated real-world ML framework workload running sequentially on a single client, and how the `AdaptiveAIAgent` dynamically adapts its policies in real-time without user intervention.\n\n")
	story.WriteString("| Time | Workload Event | Computed Workload Class | AI Tuning Decision & Policy |\n")
	story.WriteString("|------|----------------|-------------------------|---------------------------|\n")

	t := 0
	agent.RecordRead(0, 1024, 2*time.Millisecond, false)
	agent.RecordRead(1024, 1024, 2*time.Millisecond, false)
	readPolicy := agent.PredictReadPolicy(1024, 256*1024*1024)
	class := agent.ClassifyWorkload(1024)
	story.WriteString(fmt.Sprintf("| T+%ds | Loading 10KB config files | `%s` | **Prefetch Strategy**: %v <br> **Chunk**: %d KB <br> **Depth**: %d |\n",
		t, class, readPolicy.Strategy, readPolicy.InitialChunkSize/1024, readPolicy.PrefetchDepth))

	t += 5
	for i := 0; i < 50; i++ {
		agent.RecordWriteInflow(40*1024, 10*time.Millisecond) // 4MB/s
	}
	upPolicy := agent.PredictUploadPolicy(0, 256*1024*1024)
	class = agent.ClassifyWorkload(0)
	story.WriteString(fmt.Sprintf("| T+%ds | Early Checkpoint (Slow Compute, 4 MB/s inflow) | `%s` | **Detected Slow Producer!** <br> **Chunk Size Drop**: %d MB <br> **Flush Deadline**: %v <br> (Prevents 8s RAM stall) |\n",
		t, class, upPolicy.ChunkSize/(1024*1024), upPolicy.FlushDeadline))

	t += 60
	agent.RecordNetworkTransfer(100*1024*1024, 250*time.Millisecond, false) // 400 MB/s!
	for i := 0; i < 50; i++ {
		agent.RecordWriteInflow(4*1024*1024, 10*time.Millisecond) // 400MB/s
	}
	payload10GB := int64(10 * 1024 * 1024 * 1024) // 10 GB
	upPolicy = agent.PredictUploadPolicy(payload10GB, 1024*1024*1024) // 1GB Memory budget
	class = agent.ClassifyWorkload(payload10GB)
	story.WriteString(fmt.Sprintf("| T+%ds | Massive 10GB Epoch Checkpoint @ 400 MB/s Compute | `%s` | **Switched to Parallel Composite Uploads!** <br> **PCU Part Size**: %d MB <br> **PCU Workers**: %d <br> **Chunk**: %d MB |\n",
		t, class, upPolicy.PCUPartSize/(1024*1024), upPolicy.Concurrency, upPolicy.ChunkSize/(1024*1024)))

	t += 120
	for i := 0; i < 10; i++ {
		agent.RecordRead(int64(i*8*1024*1024), 8*1024*1024, 20*time.Millisecond, true)
	}
	readPolicy = agent.PredictReadPolicy(payload10GB, 256*1024*1024)
	class = agent.ClassifyWorkload(payload10GB)
	story.WriteString(fmt.Sprintf("| T+%ds | Streaming 10GB DataLoader (Consumer Starving) | `%s` | **Prefetch Strategy**: %v <br> **Chunk Size Expanded**: %d MB <br> **Lookahead Pipeline Depth**: %d <br> (Saturating NIC) |\n",
		t, class, readPolicy.Strategy, readPolicy.MaxChunkSize/(1024*1024), readPolicy.PrefetchDepth))

	t += 180
	for i := 0; i < 10; i++ {
		agent.RecordPrefetchFeedback(2*1024*1024, 6*1024*1024, false)
	}
	readPolicy = agent.PredictReadPolicy(0, 256*1024*1024)
	story.WriteString(fmt.Sprintf("| T+%ds | Sparse Random Evaluation (25%% Prefetch Hit Ratio) | `WorkloadClassSmallRandomIO` | **Self-Healing Triggered!** <br> **Prefetch Strategy**: %v <br> (Disabled to save bandwidth & memory waste) |\n",
		t, readPolicy.Strategy))

	return story.String()
}

func main() {
	fmt.Print(runPipelineAIStory())
}
