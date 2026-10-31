"""Validate the staged inventory without executing Rust or TypeScript implementation."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
import yaml

def validate(data, ts_repo, rust_root, phase='staged', validator_path=None,
             inventory_path=None, expected_inventory_sha256=None):
    errors=[]
    def check(condition,message):
        if not condition:errors.append(message)
    check(data.get('schema_version')==1,'schema_version must be 1')
    commit=data.get('source_pins',{}).get('typescript_commit','')
    check(bool(re.fullmatch(r'[0-9a-f]{40}',commit)),'missing TypeScript commit pin')
    rows=data.get('features',[])
    ids=[r.get('id') for r in rows]
    idset=set(ids)
    check(len(ids)==len(idset),'duplicate feature IDs')
    check(len(rows)>0,'empty features')
    roles={'rust_runtime','typescript_input_rust_contract','non_runtime'}
    states={'COMPAT','NATIVE','CHANGE','uncharacterized'}
    for r in rows:
        rid=r.get('id','<missing>')
        for key in ('id','role','owner','behavior','boundary','source_blobs','dependencies','platform','committed_tests','committed_docs','uncertainty','rust_mapping','evidence'):
            check(key in r,f'{rid}: missing {key}')
        check(r.get('role') in roles,f'{rid}: invalid role')
        check(bool(r.get('source_blobs')),f'{rid}: no pinned source blob')
        check(bool(r.get('uncertainty')),f'{rid}: uncertainty unrecorded')
        if r.get('role')=='non_runtime':check(bool(r.get('non_runtime_reason')),f'{rid}: missing non-runtime reason')
        for key in ('input','output','failure'):
            check(bool(r.get('boundary',{}).get(key)),f'{rid}: missing boundary.{key}')
        mapping=r.get('rust_mapping',{})
        check(mapping.get('mapping_schema')=='source_candidate/v1',f'{rid}: mapping schema mismatch')
        for key in ('status','crate_sources','requirement_ids','requirement_mapping','mapping_source'):
            check(key in mapping,f'{rid}: missing rust_mapping.{key}')
        ev=r.get('evidence',{})
        check(ev.get('classification') in states,f'{rid}: invalid classification')
        for key in ('implementation_status','comparison_status','release_status','acceptance_fixture','receipt_id','receipt_sha256','evidence_source_pin','evidence_source_sha256','related_scoped_release_evidence','change_candidate'):
            check(key in ev,f'{rid}: missing evidence.{key}')
        if ev.get('classification')=='COMPAT':
            check(ev.get('acceptance_fixture') not in (None,'none') and ev.get('receipt_id') not in (None,'none'),f'{rid}: COMPAT without fixture/receipt')
            check(ev.get('receipt_sha256') not in (None,'none') and ev.get('evidence_source_sha256') not in (None,'none'),f'{rid}: COMPAT without hashed receipt/source')
            check(ev.get('comparison_status')=='passed_pinned_current_head_typescript_rust_fixture',f'{rid}: COMPAT without executed pinned comparison')
        if ev.get('classification')=='NATIVE':
            check(bool(mapping.get('requirement_ids')),f'{rid}: NATIVE without authored Rust requirement')
            check(ev.get('acceptance_fixture') not in (None,'none') and ev.get('receipt_id') not in (None,'none'),f'{rid}: NATIVE without fixture/receipt')
            check(ev.get('receipt_sha256') not in (None,'none') and ev.get('evidence_source_pin') not in (None,'none') and ev.get('evidence_source_sha256') not in (None,'none'),f'{rid}: NATIVE without hashed receipt/source evidence')
            check(ev.get('implementation_status')=='verified_native_whole_feature',f'{rid}: NATIVE without verified whole-feature implementation')
        if ev.get('classification')=='CHANGE':
            decision=ev.get('change_candidate') or {}
            check(decision.get('status')=='reviewed_tested_user_visible_decision',f'{rid}: CHANGE lacks reviewed/tested user-visible decision')
            check(bool(decision.get('reviewed_decision_id')) and bool(decision.get('user_visible_difference')) and bool(decision.get('test_evidence')),f'{rid}: CHANGE lacks decision ID, visible difference or test evidence')
            check(bool(decision.get('decision_sources')),f'{rid}: CHANGE without source-bound decision')
            check(ev.get('acceptance_fixture') not in (None,'none') and ev.get('receipt_id') not in (None,'none') and ev.get('receipt_sha256') not in (None,'none'),f'{rid}: CHANGE without fixture/receipt')
            check(ev.get('comparison_status')!='passed_pinned_current_head_typescript_rust_fixture',f'{rid}: CHANGE cannot receive COMPAT credit')
        for dep in r.get('dependency_ids',[]):check(dep in idset and dep!=rid,f'{rid}: invalid dependency {dep}')
    graph=data.get('dependency_graph',{})
    nodes=graph.get('nodes',[])
    check(len(nodes)==len(set(nodes)) and set(nodes)==idset,'graph node coverage or duplicate mismatch')
    edges=graph.get('edges',[])
    for edge in edges:check(edge.get('from') in idset and edge.get('to') in idset,f'invalid edge: {edge}')
    order=graph.get('topological_order',[])
    check(len(order)==len(idset) and set(order)==idset,'invalid topological coverage')
    pos={rid:i for i,rid in enumerate(order)}
    for edge in edges:
        if edge.get('from') in pos and edge.get('to') in pos:
            check(pos[edge['to']]<pos[edge['from']],f'cycle/order violation: {edge}')
    memberships=[fid for g in graph.get('priority_groups',[]) for fid in g.get('feature_ids',[])]
    check(len(memberships)==len(idset) and set(memberships)==idset,'priority group coverage mismatch')
    check(len(memberships)==len(set(memberships)),'duplicate priority group membership')
    sweep=data.get('source_sweep',{})
    check(sweep.get('source_commit')==commit,'source sweep commit mismatch')
    check(len(sweep.get('http_routes',[]))==14,'HTTP route sweep incomplete')
    check(len(sweep.get('package_export_subpaths',[]))==44,'package export sweep incomplete')
    check(len(sweep.get('example_files',[]))==96,'example source sweep incomplete')
    check(len(data.get('reconciliation',{}).get('package_manifests',[]))==21,'package manifest coverage mismatch')
    check(len(data.get('reconciliation',{}).get('audited_public_docs_examples',[]))==47,'audited docs/example coverage mismatch')
    listed=subprocess.run(['git','-C',str(ts_repo),'ls-tree','-r','--name-only',commit],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
    check(listed.returncode==0,'cannot list pinned TypeScript tree')
    pinned_paths=set(listed.stdout.splitlines()) if listed.returncode==0 else set()
    pinned_examples={p for p in pinned_paths if p.startswith('examples/')}
    sweep_examples={x['source']['path'] for x in sweep.get('example_files',[])}
    check(pinned_examples==sweep_examples,'example source sweep does not equal pinned examples tree')
    pinned_manifests={p for p in pinned_paths if re.fullmatch(r'packages/[^/]+/package.json',p)}
    reconciled_manifests={x['path'] for x in data.get('reconciliation',{}).get('package_manifests',[])}
    check(pinned_manifests==reconciled_manifests,'package manifest sweep does not equal pinned tree')
    for manifest in sorted(pinned_manifests):
        raw=subprocess.run(['git','-C',str(ts_repo),'show',f'{commit}:{manifest}'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
        if raw.returncode:errors.append(f'cannot read pinned manifest {manifest}');continue
        actual=set((json.loads(raw.stdout).get('exports') or {}).keys())
        indexed={x['export_key'] for x in sweep.get('package_export_subpaths',[]) if x['manifest']['path']==manifest}
        check(actual==indexed,f'export subpath mismatch {manifest}')
    http=sweep.get('http_routes',[])
    raw_http=subprocess.run(['git','-C',str(ts_repo),'show',f'{commit}:packages/cli/src/lib/dev/runtime-fetch-core.ts'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
    if raw_http.returncode:errors.append('cannot read pinned HTTP router')
    else:
        router_lines=raw_http.stdout.splitlines()
        pinned_patterns=[(i+1,line.strip()) for i,line in enumerate(router_lines) if 1030<=i+1<=1510 and 'pattern: /^\\/' in line]
        indexed_patterns=[(x['pattern_line'],x['pattern_source']) for x in http]
        check(pinned_patterns==indexed_patterns,'HTTP route table sweep mismatch')
    for subpath in sweep.get('package_export_subpaths',[]):
        entry=subpath['entrypoint']['path']
        if entry.endswith(('.css','.json')):continue
        raw=subprocess.run(['git','-C',str(ts_repo),'show',f'{commit}:{entry}'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
        if raw.returncode:errors.append(f'cannot read pinned export entrypoint {entry}');continue
        export_lines={i+1 for i,line in enumerate(raw.stdout.splitlines()) if re.match(r'^\s*export\b',line)}
        indexed_lines={s['line'] for s in subpath['statements'] if s['source_path']==entry}
        check(export_lines==indexed_lines,f'export statement sweep mismatch {entry}')
    refs={}
    def visit(value):
        if isinstance(value,dict):
            path=value.get('path');sha=value.get('sha256')
            if isinstance(path,str) and isinstance(sha,str) and re.fullmatch(r'[0-9a-f]{64}',sha):
                if path in refs and refs[path]!=sha:errors.append(f'conflicting hash for {path}')
                refs[path]=sha
            for child in value.values():visit(child)
        elif isinstance(value,list):
            for child in value:visit(child)
    visit(data)
    for path,expected in sorted(refs.items()):
        result=subprocess.run(['git','-C',str(ts_repo),'show',f'{commit}:{path}'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
        if result.returncode==0:
            actual=hashlib.sha256(result.stdout).hexdigest()
        else:
            target=rust_root/path
            if not target.is_file():errors.append(f'missing source reference {path}');continue
            actual=hashlib.sha256(target.read_bytes()).hexdigest()
        check(actual==expected,f'source hash mismatch {path}: expected {expected}, got {actual}')
    root_pin=data.get('source_pins',{}).get('typescript_root_package_sha256')
    lock_pin=data.get('source_pins',{}).get('typescript_pnpm_lock_sha256')
    for path,expected in [('package.json',root_pin),('pnpm-lock.yaml',lock_pin)]:
        result=subprocess.run(['git','-C',str(ts_repo),'show',f'{commit}:{path}'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
        check(result.returncode==0 and hashlib.sha256(result.stdout).hexdigest()==expected,f'freeze pin mismatch {path}')
    pins=data.get('source_pins',{})
    for path,key in [('spec/runtime/requirements.yaml','rust_requirements_sha256'),
                     ('spec/runtime/compatibility.md','rust_compatibility_sha256')]:
        target=rust_root/path
        check(target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest()==pins.get(key),f'Rust document pin mismatch {path}')
    listed_rust=subprocess.run(['rg','--files','--hidden','-g','!.git/**','-g','!.b4-work/**'],cwd=rust_root,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
    check(listed_rust.returncode==0,'cannot list Rust public source map')
    if listed_rust.returncode==0:
        rust_paths=set(listed_rust.stdout.splitlines())
        vendor='vendor/rusqlite/Cargo.lock'
        if (rust_root/vendor).is_file():rust_paths.add(vendor)
        boundary=data.get('publication_boundary',{})
        proposed={'scripts/validate_full_runtime_feature_inventory.py','spec/runtime/full-runtime-feature-inventory.yaml'}
        named=set(boundary.get('new_public_paths',[]))
        check(named==proposed,'publication path boundary must name exactly the two reviewed additions')
        present=rust_paths & proposed
        check(phase in ('staged','published'),'phase must be staged or published')
        if phase=='staged':
            check(not present,f'staged phase has published path(s): {sorted(present)}')
        elif phase=='published':
            check(present==proposed,f'published phase missing expected path(s): {sorted(proposed-present)}')
            check(len(rust_paths)==boundary.get('published_public_file_count'),'published public file count mismatch')
            check(bool(expected_inventory_sha256),'published phase requires external reviewed inventory SHA-256')
            if inventory_path is not None and expected_inventory_sha256:
                actual_inventory=hashlib.sha256(Path(inventory_path).read_bytes()).hexdigest()
                check(actual_inventory==expected_inventory_sha256,'published inventory differs from external reviewed SHA-256')
            if inventory_path is not None:
                check(Path(inventory_path).resolve()==(rust_root/'spec/runtime/full-runtime-feature-inventory.yaml').resolve(),'published phase must validate installed public inventory')
            if validator_path is not None:
                check(Path(validator_path).resolve()==(rust_root/'scripts/validate_full_runtime_feature_inventory.py').resolve(),'published phase must run installed public validator')
        validator_path=Path(validator_path) if validator_path is not None else Path(__file__)
        check(validator_path.is_file() and hashlib.sha256(validator_path.read_bytes()).hexdigest()==boundary.get('validator_sha256'),'validator artifact hash mismatch')
        predecessor_paths=rust_paths-proposed
        frozen=[]
        for path in sorted(predecessor_paths):
            frozen.append(f'{path}\0{hashlib.sha256((rust_root/path).read_bytes()).hexdigest()}')
        digest=hashlib.sha256(('\n'.join(frozen)+'\n').encode()).hexdigest()
        check(len(predecessor_paths)==boundary.get('predecessor_public_file_count')==pins.get('rust_public_file_count'),'accepted predecessor file count mismatch')
        check(digest==boundary.get('predecessor_public_map_sha256')==pins.get('rust_public_source_map_sha256'),'accepted predecessor source map hash mismatch')
    return errors,len(refs)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--inventory',type=Path,required=True)
    parser.add_argument('--typescript-repo',type=Path,required=True)
    parser.add_argument('--rust-root',type=Path,required=True)
    parser.add_argument('--phase',choices=('staged','published'),required=True)
    parser.add_argument('--expected-inventory-sha256',help='External reviewed YAML digest; required for published phase')
    args=parser.parse_args()
    data=yaml.safe_load(args.inventory.read_text())
    errors,refs=validate(data,args.typescript_repo,args.rust_root,phase=args.phase,
                         validator_path=Path(__file__),inventory_path=args.inventory,
                         expected_inventory_sha256=args.expected_inventory_sha256)
    if errors:
        for e in errors:print('ERROR',e)
        raise SystemExit(1)
    print(f'VALID phase={args.phase} features={len(data["features"])} source_refs={refs} http_routes={len(data["source_sweep"]["http_routes"])} subpaths={len(data["source_sweep"]["package_export_subpaths"])} examples={len(data["source_sweep"]["example_files"])}')

if __name__=='__main__':main()
