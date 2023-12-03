extern crate winres;

fn main() {
    slint_build::compile("ui/main.slint").unwrap();

    if cfg!(target_os = "windows") {
        let mut res = winres::WindowsResource::new();
        res.set_icon("ui/table.ico");
        res.compile().unwrap();
      }
}
