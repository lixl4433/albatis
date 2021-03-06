package net.butfly.albatis.io.ext;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.OpenableThread;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Queue;
import net.butfly.albatis.io.WrapOutput;

/**
 * Output with buffer and pool supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback provided by children classes.<br>
 * Children classes define and implemented connection to datasource.
 * 
 * @author zx
 *
 * @param <M>
 */
public class FailoverOutput<M> extends WrapOutput<M, M> {
	private static final long serialVersionUID = -222291630780849828L;
	protected final Queue<M> pool;
	protected final OpenableThread failovering;

	public FailoverOutput(Output<M> output, Queue<M> failover) {
		super(output, "Failover");
		this.pool = failover;
		failovering = new OpenableThread(() -> {
			while (output.opened() && opened()) {
				while (failover.empty())
					if (!output.opened() || !opened() || !Task.waitSleep()) break;
				failover.dequeue(output::enqueue);
			}
			logger().info("Failovering stopped.");
		}, name() + "Failovering");
		closing(() -> {
			failovering.close();
			if (!pool.empty()) logger().warn("Failovering pool not empty [" + pool.size() + "], maybe lost.");
			else logger().info("Failovering pool empty and closing.");
			pool.close();
		});
		pool.open();
	}

	@Override
	public void open() {
		super.open();
		failovering.open();
		base.open();
	}

	@Override
	public final void enqueue(Sdream<M> els) {
		base.enqueue(els);
	}

	@Override
	public void failed(Sdream<M> failed) {
		pool.enqueue(failed);
	}

	public final long fails() {
		return pool.size();
	}

	@Override
	public String toString() {
		return super.toString() + "[fails: " + fails() + "]";
	}
}