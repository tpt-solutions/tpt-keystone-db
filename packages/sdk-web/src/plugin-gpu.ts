// Custom rendering hooks for Canvas plugins (TODO.md Phase 14, "Custom
// rendering hooks (WebGPU compute + fragment shaders)"). `tpt-canvas`'s
// Rust core deliberately renders via `web_sys::CanvasRenderingContext2d`
// rather than WebGPU (see `tpt-canvas/src/lib.rs`'s documented scope cut),
// so there is no Rust-side shader pipeline for a plugin to hook into.
// This module instead talks to the browser's native WebGPU API directly
// from the JS/TS plugin layer — a real compute + fragment-shader pipeline,
// just one layer up from where `tpt-canvas` chose to stop.
//
// TypeScript's bundled DOM lib doesn't ship WebGPU types (still a separate,
// fast-moving `@webgpu/types` package this repo doesn't depend on), so the
// interfaces below are a deliberately minimal structural subset covering
// only what `CanvasGpuContext` actually calls.
//
// This module only runs where `navigator.gpu` exists (a real browser with
// WebGPU enabled) — it cannot be exercised by `node --test` the way
// `plugin-events.ts`/`plugin.ts` can, mirroring the same DOM-availability
// limitation `tpt-canvas`'s Phase 13 milestone already documents.

interface GpuAdapterLike {
  requestDevice(): Promise<GpuDeviceLike>;
}

interface GpuQueueLike {
  submit(commandBuffers: unknown[]): void;
  writeBuffer(buffer: GpuBufferLike, bufferOffset: number, data: ArrayBufferView): void;
}

export interface GpuBufferLike {
  readonly size: number;
  mapAsync(mode: number): Promise<void>;
  getMappedRange(): ArrayBuffer;
  unmap(): void;
  destroy(): void;
}

interface GpuDeviceLike {
  readonly queue: GpuQueueLike;
  createShaderModule(desc: { code: string }): unknown;
  createBuffer(desc: { size: number; usage: number; mappedAtCreation?: boolean }): GpuBufferLike;
  createComputePipeline(desc: unknown): { getBindGroupLayout(index: number): unknown };
  createRenderPipeline(desc: unknown): unknown;
  createBindGroup(desc: unknown): unknown;
  createCommandEncoder(): GpuCommandEncoderLike;
}

interface GpuCommandEncoderLike {
  beginComputePass(): GpuComputePassLike;
  beginRenderPass(desc: unknown): GpuRenderPassLike;
  copyBufferToBuffer(src: GpuBufferLike, srcOffset: number, dst: GpuBufferLike, dstOffset: number, size: number): void;
  finish(): unknown;
}

interface GpuComputePassLike {
  setPipeline(pipeline: unknown): void;
  setBindGroup(index: number, bindGroup: unknown): void;
  dispatchWorkgroups(x: number, y?: number, z?: number): void;
  end(): void;
}

interface GpuRenderPassLike {
  setPipeline(pipeline: unknown): void;
  draw(vertexCount: number): void;
  end(): void;
}

interface GpuCanvasContextLike {
  configure(config: unknown): void;
  getCurrentTexture(): { createView(): unknown };
}

interface NavigatorGpuLike {
  requestAdapter(): Promise<GpuAdapterLike | null>;
  getPreferredCanvasFormat?(): string;
}

// WebGPU flag values are stable across implementations; declared locally
// so callers don't need `@webgpu/types`' global `GPUBufferUsage`/`GPUMapMode`.
export const GpuBufferUsage = {
  MAP_READ: 0x0001,
  COPY_SRC: 0x0004,
  COPY_DST: 0x0008,
  STORAGE: 0x0080,
} as const;

export const GpuMapMode = {
  READ: 0x0001,
} as const;

export function isWebGpuSupported(): boolean {
  return typeof navigator !== "undefined" && "gpu" in navigator;
}

export interface ComputeBufferSpec {
  data: Float32Array | Uint32Array | Int32Array;
  /** Read the buffer back to the CPU after dispatch (default: true). */
  readback?: boolean;
}

export interface RunComputeOptions {
  entryPoint?: string;
  workgroups: [number, number?, number?];
  buffers: ComputeBufferSpec[];
}

export interface RenderFragmentOptions {
  vertexEntryPoint?: string;
  fragmentEntryPoint?: string;
  /** Defaults to 3 — a full-screen triangle driven entirely by `vertex_index`. */
  vertexCount?: number;
}

export class CanvasGpuContext {
  constructor(
    private readonly device: GpuDeviceLike,
    private readonly context: GpuCanvasContextLike,
    private readonly format: string,
  ) {}

  /** Runs a WGSL compute shader over storage buffers, returning each readback-flagged buffer's bytes. */
  async runCompute(wgsl: string, opts: RunComputeOptions): Promise<ArrayBuffer[]> {
    const module = this.device.createShaderModule({ code: wgsl });
    const pipeline = this.device.createComputePipeline({
      layout: "auto",
      compute: { module, entryPoint: opts.entryPoint ?? "main" },
    });

    const gpuBuffers = opts.buffers.map((spec) => {
      const buffer = this.device.createBuffer({
        size: spec.data.byteLength,
        usage: GpuBufferUsage.STORAGE | GpuBufferUsage.COPY_SRC | GpuBufferUsage.COPY_DST,
      });
      this.device.queue.writeBuffer(buffer, 0, spec.data);
      return { buffer, spec };
    });

    const bindGroup = this.device.createBindGroup({
      layout: pipeline.getBindGroupLayout(0),
      entries: gpuBuffers.map((gb, index) => ({ binding: index, resource: { buffer: gb.buffer } })),
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginComputePass();
    pass.setPipeline(pipeline);
    pass.setBindGroup(0, bindGroup);
    pass.dispatchWorkgroups(...opts.workgroups);
    pass.end();

    const readbacks = gpuBuffers
      .filter((gb) => gb.spec.readback !== false)
      .map((gb) => {
        const staging = this.device.createBuffer({
          size: gb.buffer.size,
          usage: GpuBufferUsage.MAP_READ | GpuBufferUsage.COPY_DST,
        });
        encoder.copyBufferToBuffer(gb.buffer, 0, staging, 0, gb.buffer.size);
        return staging;
      });

    this.device.queue.submit([encoder.finish()]);

    const results: ArrayBuffer[] = [];
    for (const staging of readbacks) {
      await staging.mapAsync(GpuMapMode.READ);
      results.push(staging.getMappedRange().slice(0));
      staging.unmap();
      staging.destroy();
    }
    for (const gb of gpuBuffers) gb.buffer.destroy();
    return results;
  }

  /** Runs a vertex+fragment shader pair against the plugin's own WebGPU canvas. */
  renderFragment(vertexWgsl: string, fragmentWgsl: string, opts: RenderFragmentOptions = {}): void {
    const vertexModule = this.device.createShaderModule({ code: vertexWgsl });
    const fragmentModule = this.device.createShaderModule({ code: fragmentWgsl });

    const pipeline = this.device.createRenderPipeline({
      layout: "auto",
      vertex: { module: vertexModule, entryPoint: opts.vertexEntryPoint ?? "vs_main" },
      fragment: {
        module: fragmentModule,
        entryPoint: opts.fragmentEntryPoint ?? "fs_main",
        targets: [{ format: this.format }],
      },
      primitive: { topology: "triangle-list" },
    });

    const encoder = this.device.createCommandEncoder();
    const pass = encoder.beginRenderPass({
      colorAttachments: [
        {
          view: this.context.getCurrentTexture().createView(),
          loadOp: "clear",
          storeOp: "store",
          clearValue: { r: 0, g: 0, b: 0, a: 0 },
        },
      ],
    });
    pass.setPipeline(pipeline);
    pass.draw(opts.vertexCount ?? 3);
    pass.end();
    this.device.queue.submit([encoder.finish()]);
  }
}

export async function createGpuContext(canvas: HTMLCanvasElement): Promise<CanvasGpuContext> {
  const nav = navigator as unknown as { gpu?: NavigatorGpuLike };
  if (!nav.gpu) {
    throw new Error("WebGPU is not supported in this environment (navigator.gpu is undefined)");
  }
  const adapter = await nav.gpu.requestAdapter();
  if (!adapter) {
    throw new Error("No WebGPU adapter is available");
  }
  const device = await adapter.requestDevice();
  const context = canvas.getContext("webgpu") as unknown as GpuCanvasContextLike | null;
  if (!context) {
    throw new Error("Failed to acquire a webgpu canvas context (does this canvas already have a 2d/webgl context?)");
  }
  const format = nav.gpu.getPreferredCanvasFormat?.() ?? "bgra8unorm";
  context.configure({ device, format, alphaMode: "opaque" });
  return new CanvasGpuContext(device, context, format);
}
