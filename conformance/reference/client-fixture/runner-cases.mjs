export const CASES = Object.freeze({
  C1: ['factory-create', 'factory-dispatch', 'factory-approve', 'factory-retry', 'factory-deny',
    'factory-intake', 'factory-approve-intake', 'factory-reject-intake', 'factory-cancel', 'factory-reconcile'],
  C2: ['default-auth-refusal', 'bearer-auth-positive'],
  C3: ['wait-value', 'wait-failure', 'wait-void'],
  C4: ['held-dispatch-abandon', 'held-busy-before-abort', 'held-busy-after-abort', 'interrupt-abandoned-held',
    'held-dispatch-cancel', 'interrupt-held', 'held-busy-after-cancel', 'interrupt-idle', 'interrupt-missing',
    'concealed-establish', 'interrupt-concealed'],
  W1: ['worker-create', 'worker-get', 'worker-get-missing', 'worker-get-concealed', 'worker-get-wrong-token',
    'worker-delete', 'worker-delete-missing', 'worker-held-create', 'worker-held-start', 'worker-delete-held', 'worker-delete-released'],
  W2: ['value-create', 'value-start', 'void-create', 'void-start', 'failure-create', 'failure-start'],
  W3: ['attach-create', 'attach-held-start', 'attach-live', 'attach-busy-after-viewer-cut', 'attach-busy-after-post-abort',
    'attach-cancel-create', 'attach-cancel-held-start', 'worker-cancel-held', 'attach-busy-after-cancel', 'attach-settled',
    'worker-cancel-idle', 'worker-cancel-missing', 'worker-cancel-concealed'],
  W4: ['park-create', 'pending-empty', 'park-start', 'pending-original-schema-refusal',
    'resume-forced-once', 'resume-forced-deny', 'resume-empty'],
});

export const SELECTED_METHODS = Object.freeze([
  'controller.create', 'controller.dispatch', 'controller.approve', 'controller.retry', 'controller.deny',
  'controller.intake', 'controller.approveIntake', 'controller.rejectIntake', 'controller.cancel', 'controller.reconcile', 'controller.interrupt',
  'worker.createThread', 'worker.getThread', 'worker.deleteThread', 'worker.pendingInterrupts', 'worker.startRun',
  'worker.reattach', 'worker.resume', 'worker.cancel',
]);
