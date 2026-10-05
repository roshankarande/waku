package providers

var allAccess = []AccessMode{AccessAsk, AccessAutoAcceptEdits, AccessAuto, AccessFull}

// Descriptors is an availability gate, not a roadmap. Unimplemented adapters
// deliberately expose no capabilities.
func Descriptors() map[Provider]Descriptor {
	return map[Provider]Descriptor{
		ProviderCodex: {
			Provider: ProviderCodex, Binary: "codex", Transport: "stdio-jsonrpc", Implemented: true,
			Capabilities: Capabilities{
				Reasoning: true, ServiceTier: true, Approvals: true, Questions: true,
				Steering: true, Resume: true, Fork: true, Rewind: true, Goals: true,
				AccessModes: append([]AccessMode(nil), allAccess...), Cancellation: CancellationDrop,
			},
		},
		ProviderCursor: acpDescriptor(ProviderCursor, "cursor-agent", true, true),
		ProviderFx:     acpDescriptor(ProviderFx, "fx", false, false),
		ProviderGrok:   acpDescriptor(ProviderGrok, "grok", true, true),
		ProviderKimi:   acpDescriptor(ProviderKimi, "kimi", true, false),
		ProviderPi:     piDescriptor(ProviderPi, "pi"),
		ProviderOhMyPi: piDescriptor(ProviderOhMyPi, "omp"),
		ProviderClaude: {
			Provider: ProviderClaude, Binary: "claude", Transport: "stdio-stream-json", Implemented: true,
			Capabilities: Capabilities{
				Reasoning: true, Approvals: true, Questions: true, Steering: true,
				Resume:      true,
				AccessModes: append([]AccessMode(nil), allAccess...), Cancellation: CancellationProtocol,
			},
		},
		ProviderAmp: {
			Provider: ProviderAmp, Binary: "amp", Transport: "stdio-stream-json", Implemented: true,
			Capabilities: Capabilities{
				Reasoning: true, Steering: true, Resume: true,
				AccessModes: []AccessMode{AccessFull}, Cancellation: CancellationDrop,
			},
		},
		ProviderOpenCode: {
			Provider: ProviderOpenCode, Binary: "opencode", Transport: "http-sse", Implemented: true,
			Capabilities: Capabilities{Models: true, Reasoning: true, Approvals: true, Questions: true, Steering: true, Resume: true, AccessModes: append([]AccessMode(nil), allAccess...), Cancellation: CancellationProtocol},
		},
		ProviderDeepSeek: {
			Provider: ProviderDeepSeek, Binary: "dsh", Transport: "http-downlink", Implemented: true,
			Capabilities: Capabilities{
				Approvals: true, Questions: true, Steering: true, Resume: true,
				AccessModes: []AccessMode{AccessAsk}, Cancellation: CancellationProtocol,
			},
		},
	}
}

func piDescriptor(provider Provider, binary string) Descriptor {
	fork := provider == ProviderPi
	return Descriptor{
		Provider: provider, Binary: binary, Transport: "stdio-ndjson-rpc", Implemented: true,
		Capabilities: Capabilities{
			Models: true, Reasoning: true, Steering: true, Resume: true, Fork: fork, Rewind: true,
			AccessModes: []AccessMode{AccessFull}, Cancellation: CancellationProtocol,
		},
	}
}

func acpDescriptor(provider Provider, binary string, steering, questions bool) Descriptor {
	return Descriptor{
		Provider: provider, Binary: binary, Transport: "stdio-acp-jsonrpc", Implemented: true,
		Capabilities: Capabilities{
			Approvals: true, Questions: questions, Steering: steering,
			Resume:      true,
			AccessModes: append([]AccessMode(nil), allAccess...), Cancellation: CancellationProtocol,
		},
	}
}

func unavailable(provider Provider, binary, transport string) Descriptor {
	return Descriptor{Provider: provider, Binary: binary, Transport: transport}
}
