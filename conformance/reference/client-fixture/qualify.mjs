// Read-only offline qualification; no original caller is imported.
import { loadEvidenceRole, readPrivateJson, loadCapture } from './artifacts.mjs';
import { auditNativeArtifacts } from './native-audit.mjs';
import { auditClientTraffic } from './client-audit.mjs';
import { projectClientObservations } from './projection.mjs';

export async function qualifyClientArtifacts(nativeDir, clientDir) {
  const native = await loadEvidenceRole(nativeDir, 'native');
  const client = await loadEvidenceRole(clientDir, 'client');
  const rawClient = readPrivateJson(client.files, 'client-observations.json');
  const { ready, retirement } = auditNativeArtifacts(native, client, rawClient);
  const captures = loadCapture(native);
  const { groups } = auditClientTraffic(rawClient, captures, ready);
  return projectClientObservations({ version: 1, platform: 'macos-arm64',
    source_commit: '8acac534ba6eb960b34de217b0da45b8405023af', groups, retirement });
}
