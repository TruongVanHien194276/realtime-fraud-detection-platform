// A short, honest evidence montage from saved local benchmark artifacts.
// This is not a recording of a live production transaction flow.
import AppKit
import Foundation
import ImageIO
import UniformTypeIdentifiers

let width: CGFloat = 1280
let height: CGFloat = 720
let root = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
let output = root.appendingPathComponent("demo/week10-evidence.gif")

func loadJSON(_ relative: String) -> [String: Any] {
    let url = root.appendingPathComponent(relative)
    guard let data = try? Data(contentsOf: url),
          let value = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
        return [:]
    }
    return value
}

let stream100 = loadJSON("reports/week10/stream-100-r1.json")
let stream500 = loadJSON("reports/week10/stream-500-r1.json")
let stream1000 = loadJSON("reports/week10/stream-1000-r1.json")
let failure = loadJSON("reports/week10/stream-failure.json")
let model = loadJSON("reports/week10/api-model-info.json")
let decision = loadJSON("reports/week10/decision-evidence.json")
let decisionCounts = decision["decision_counts"] as? [String: Any] ?? [:]
let outboxCounts = decision["outbox_topic_counts"] as? [String: Any] ?? [:]

func integer(_ value: Any?) -> String {
    guard let number = value as? NSNumber else { return "not measured" }
    return number.stringValue
}

struct Frame {
    let title: String
    let body: String
    let image: String?
}

let frames: [Frame] = [
    Frame(title: "Fraud platform — evidence montage",
          body: "Apple M3 · 8 GiB RAM\nLocal PaySim replay, not production traffic\nBatch + streaming + rules + separate hybrid API",
          image: nil),
    Frame(title: "Normal replay and duplicate protection",
          body: "100 EPS offered burst\nSilver unique IDs: \(integer(stream100["distinct_transaction_ids"]))\nP95 producer-to-batch-start: \(integer(stream100["processing_delay_p95_ms"])) ms\nTransaction ID is the Iceberg dedupe key",
          image: nil),
    Frame(title: "Fraud/invalid-event investigation",
          body: "Historical rules run: \(integer(decisionCounts["APPROVE"])) APPROVE, \(integer(decisionCounts["REVIEW"])) REVIEW, \(integer(decisionCounts["BLOCK"])) BLOCK\nAlert outbox rows: \(integer(outboxCounts["fraud.alerts"]))\n500/1,000 offered-EPS p95: \(integer(stream500["processing_delay_p95_ms"])) / \(integer(stream1000["processing_delay_p95_ms"])) ms\nDLQ: late, malformed, unsupported schema\nRestart verifier: \((failure["independent_verifier"] as? [String: Any])?["result"] ?? "not run")",
          image: nil),
    Frame(title: "Operations dashboard — recorded screenshot",
          body: "Grafana: persisted decisions, alerts, pipeline health\nHistorical replay latency is not live serving latency",
          image: "reports/screenshots/week9-grafana.png"),
    Frame(title: "Versioned hybrid scoring API",
          body: "Model: \(model["name"] ?? "not measured")\nAlias: \(model["alias"] ?? "not measured")\nVersion: \(model["version"] ?? "not measured")\nSlow inference returns degraded rule-only fallback",
          image: nil),
    Frame(title: "Limits and reproducibility",
          body: "One broker, one partition, one API worker\nOffered EPS is not sustained throughput\nNo end-to-end exactly-once claim\nSee README, performance report, tests, and ADRs",
          image: nil),
]

func makeImage(_ frame: Frame) -> CGImage {
    let canvas = NSImage(size: NSSize(width: width, height: height))
    canvas.lockFocus()
    NSColor(calibratedRed: 0.055, green: 0.075, blue: 0.12, alpha: 1).setFill()
    NSBezierPath(rect: NSRect(x: 0, y: 0, width: width, height: height)).fill()

    let titleStyle: [NSAttributedString.Key: Any] = [
        .font: NSFont.boldSystemFont(ofSize: 42),
        .foregroundColor: NSColor.white,
    ]
    let bodyStyle: [NSAttributedString.Key: Any] = [
        .font: NSFont.systemFont(ofSize: 27),
        .foregroundColor: NSColor(calibratedRed: 0.79, green: 0.88, blue: 0.97, alpha: 1),
    ]
    (frame.title as NSString).draw(in: NSRect(x: 55, y: 615, width: 1170, height: 65),
                                   withAttributes: titleStyle)
    let bodyWidth: CGFloat = frame.image == nil ? 1120 : 450
    (frame.body as NSString).draw(in: NSRect(x: 55, y: 185, width: bodyWidth, height: 360),
                                  withAttributes: bodyStyle)

    if let imagePath = frame.image,
       let screenshot = NSImage(contentsOfFile: root.appendingPathComponent(imagePath).path) {
        screenshot.draw(in: NSRect(x: 560, y: 130, width: 665, height: 445),
                        from: NSRect(origin: .zero, size: screenshot.size),
                        operation: .sourceOver, fraction: 1)
    }
    ("Local evidence · 2026-10-16" as NSString).draw(
        in: NSRect(x: 55, y: 35, width: 700, height: 35), withAttributes: bodyStyle)
    canvas.unlockFocus()
    guard let tiff = canvas.tiffRepresentation,
          let bitmap = NSBitmapImageRep(data: tiff),
          let cgImage = bitmap.cgImage else {
        fatalError("Could not render GIF frame")
    }
    return cgImage
}

FileManager.default.createFile(atPath: output.path, contents: nil)
guard let destination = CGImageDestinationCreateWithURL(
    output as CFURL, UTType.gif.identifier as CFString, frames.count, nil
) else { fatalError("Could not create GIF destination") }
CGImageDestinationSetProperties(destination, [
    kCGImagePropertyGIFDictionary: [kCGImagePropertyGIFLoopCount: 0]
] as CFDictionary)
for frame in frames {
    CGImageDestinationAddImage(destination, makeImage(frame), [
        kCGImagePropertyGIFDictionary: [kCGImagePropertyGIFDelayTime: 4.0]
    ] as CFDictionary)
}
guard CGImageDestinationFinalize(destination) else { fatalError("Could not write GIF") }
print("Wrote \(output.path)")
