use super::LevelsController;

pub fn helper_dump_levels(lvctl: &LevelsController) {
    for level in &lvctl.core.levels {
        let level = level.read().unwrap();
        println!("--- Level {} ---", level.level);
        for table in &level.tables {
            println!(
                "#{} ({:?} - {:?}, {})",
                table.id(),
                table.smallest(),
                table.biggest(),
                table.size()
            );
        }
    }
}

#[test]
fn test_levels() {

}
