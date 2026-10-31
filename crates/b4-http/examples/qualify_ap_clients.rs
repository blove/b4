//! Private, provider-free qualification of the unchanged repository AP clients.
mod ap_client_support;
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).take(5).collect();
    if args.len() != 4 {
        eprintln!("ap_client_fixture_arguments");
        std::process::exit(2);
    }
    let result = ap_client_support::run(ap_client_support::Args {
        node: args[0].clone().into(),
        runner: args[1].clone().into(),
        bundle_dir: args[2].clone().into(),
        output_dir: args[3].clone().into(),
    })
    .await;
    if let Err(error) = result {
        eprintln!("{}", error.0);
        std::process::exit(1);
    }
}
