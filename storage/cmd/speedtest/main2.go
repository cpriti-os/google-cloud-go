package main
import "fmt"
import "cloud.google.com/go/storage"
func main() {
    aiAgent := storage.GetGlobalAIAgent()
    policy := aiAgent.PredictUploadPolicy(10*1024*1024*1024, 1024*1024*1024)
    fmt.Printf("Policy for 10GB: PartSize=%d, Concurrency=%d\n", policy.PCUPartSize, policy.Concurrency)
}
