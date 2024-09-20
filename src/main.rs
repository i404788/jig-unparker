use std::{hash::Hash, thread::JoinHandle, time::Duration};

use rand_xoshiro::rand_core::{RngCore, SeedableRng};

#[derive(Copy, Clone)]
#[repr(packed(1))]
struct Piece {
    /// Winding order is CW, the fist bitset (left-aligned) is left of the piece, second is top, third is right, fourth is bottom
    ///  i.e. <LEFT> . <TOP> . <RIGHT> . <BOTTOM>
    conntype: u64,
    // To simplify rotations directions are encoded as IN=0b01, OUT=0b10, DONTCARE=0b11, EDGE=0b00
    //  it uses the same clock ordering as conntype
    direction: DirectionType,
}

impl Default for Piece {
    fn default() -> Self {
        Piece {
            conntype: 0u64,
            direction: DirectionType(0xffu8),
        }
    }
}

#[derive(Clone)]
struct JigPuzzle {
    // board: Array2D<Piece>,
    board: Vec<Piece>,
    rows: usize,
    columns: usize,
}

impl JigPuzzle {
    fn new_empty(rows: usize, columns: usize) -> JigPuzzle {
        JigPuzzle {
            // board: Array2D::filled_with(Piece::default(), rows, columns),
            board: vec![Piece::default(); rows * columns],
            rows,
            columns,
        }
    }

    fn get(&self, xy: (usize, usize)) -> Option<&Piece> {
        if xy.0 >= self.rows || xy.1 >= self.columns {
            None
        } else {
            Some(unsafe { self.board.get_unchecked(xy.0 * self.columns + xy.1) })
        }
    }

    fn deconstruct(self) -> Vec<Piece> {
        self.board
    }

    unsafe fn place_unchecked(&mut self, x: usize, y: usize, piece: Piece) {
        #[cfg(debug_assertions)]
        let piece_ref = self.board.get_mut(x * self.columns + y).unwrap();
        #[cfg(not(debug_assertions))]
        let piece_ref = self.board.get_unchecked_mut(x * self.columns + y);
        *piece_ref = piece;
    }
}

impl Hash for JigPuzzle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Take our board as a byte array (with packed(1) should be as exact as needed)
        // Technically we don't need to process the whole board but might as well
        let byte_ref = self.board.as_ptr() as *const u8;
        let byte_vec = unsafe {
            std::slice::from_raw_parts(byte_ref, self.board.len() * std::mem::size_of::<Piece>())
        };
        state.write(byte_vec);
    }
}
impl PartialEq for JigPuzzle {
    fn eq(&self, other: &Self) -> bool {
        if self.rows != other.rows || self.columns != other.rows {
            false
        } else {
            self.board
                .iter()
                .zip(other.board.iter())
                .find(|(a, b)| a.conntype != b.conntype)
                .is_none()
        }
    }
}
impl Eq for JigPuzzle {}

#[derive(Copy, Clone, Debug)]
struct DirectionType(u8);

impl Default for DirectionType {
    #[inline]
    fn default() -> Self {
        DirectionType(0)
    }
}

mod swar {
    // https://lamport.azurewebsites.net/pubs/multiple-byte.pdf
    /// Output in the lowest bit of the N (0x55 for N=2)
    pub fn multi_lt<const N: u16>(a: u16, b: u16) -> u16 {
        let m = (1 << (N - 1)) - 1;
        let m = m << N | m;
        let m = m << (N * 2) | m;
        let m = m << (N * 4) | m;

        let x = (!a & m) ^ (b & m);
        return (x & !a) | (x & b) | (!a & b);
    }

    pub fn multi_eq<const N: u16>(a: u16, b: u16) -> u16 {
        // TODO: improve mask gen
        let m = (1 << (N - 1)) - 1;
        let m = m << N | m;
        let m = m << (N * 2) | m;
        let m = m << (N * 4) | m;

        let x = a ^ b ^ u16::MAX;
        let y = x & m;
        (y + (!m >> (N - 1))) & x
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum UnpackedDirectionType {
    In = 0b01,
    Out = 0b10,
    Edge = 0b00,
    DontCare = 0b11,
}

impl UnpackedDirectionType {
    fn invert(self) -> UnpackedDirectionType {
        unsafe { std::mem::transmute(self as u8 ^ 0b11) }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
enum Direction {
    Left = 3,
    Top = 2,
    Right = 1,
    Bottom = 0,
}

impl Direction {
    fn opposite(&self) -> Direction {
        unsafe { std::mem::transmute((*self as u8 + 2) % 4) }
    }

    fn translate_cell(&self, point: (usize, usize)) -> (usize, usize) {
        match self {
            Self::Left => (point.0.wrapping_sub(1), point.1),
            Self::Top => (point.0, point.1.wrapping_sub(1)),
            Self::Right => (point.0 + 1, point.1),
            Self::Bottom => (point.0, point.1 + 1),
        }
    }
}

impl DirectionType {
    #[inline]
    fn unpack_dir(&self, dir: Direction) -> UnpackedDirectionType {
        let raw = (self.0 >> (2 * dir as u8)) & 0b11;
        unsafe { std::mem::transmute(raw) }
    }

    #[inline]
    fn from_lurb(lurb: [DirectionType; 4]) -> DirectionType {
        let mut res = 0u8;
        // We are iteration over our neighbours so the read is 180deg out of phase
        // We start at our left, which we need their right (index 2)
        for (i, v) in lurb.iter().enumerate() {
            let i = (i + 2) % 4;
            res |= v.0 & (0b11 << (i * 2));
        }
        DirectionType(res)
    }

    fn rotate(&self, count: u32) -> DirectionType {
        debug_assert!(count <= 4, "There is no reason to do this");
        DirectionType(self.0.rotate_right(count * 2))
    }

    // Check if it matches in R(2) space, using cached dontcare mask
    fn matches_r2(&self, mask: &DirectionType) -> u8 {
        let mut result = 0u8;
        let is_dontcare = swar::multi_eq::<2>(mask.0 as u16, 0xff) & 0x55;
        let dontcare_mask = !(is_dontcare << 1 | is_dontcare) as u8; // Make dense

        for i in 0..4 {
            let is_eq = (self.0.rotate_right(2) & dontcare_mask) == (mask.0 & dontcare_mask);
            result |= (is_eq as u8) << i;
        }
        return result;
    }

    #[inline]
    fn matches(&self, mask: &DirectionType) -> bool {
        // use SWAR to do 4 2-bit equalities at the same time
        let is_dontcare = swar::multi_eq::<2>(mask.0 as u16, 0xff) & 0x55;
        let dontcare_mask = (is_dontcare << 1 | is_dontcare) as u8; // Make dense
        return (self.0 & !dontcare_mask) == (mask.0 & !dontcare_mask);
    }

    #[inline]
    fn care_mask(&self) -> u64 {
        let mut res = 0u64;
        for i in 0..4 {
            match self.0 & (0b11 << i) {
                0b00..=0b10 => res |= 0xffff << i * 16,
                _ => {}
            }
        }
        res
    }
}

impl Piece {
    fn rotate(&self, count: u32) -> Piece {
        debug_assert!(count <= 4, "There is no reason to do this");

        Piece {
            direction: self.direction.rotate(count),
            conntype: self.conntype.rotate_right(count * 16),
        }
    }

    fn matches(&self, rhs: u64, caremask: u64) -> bool {
        (self.conntype & caremask) == (rhs & caremask)
    }

    fn unpack_type_dir(&self, dir: Direction) -> u64 {
        (self.conntype >> (dir as u8 * 16)) & 0xffff
    }
}

impl std::fmt::Debug for Piece {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "←{:?} ↑{:?} ↓{:?} →{:?}",
            self.direction.unpack_dir(Direction::Left),
            self.direction.unpack_dir(Direction::Top),
            self.direction.unpack_dir(Direction::Right),
            self.direction.unpack_dir(Direction::Bottom)
        ))
    }
}

impl JigPuzzle {
    #[inline]
    fn get_piecemask(&self, x: usize, y: usize) -> (u64, DirectionType) {
        let left = self
            .get((x.wrapping_sub(1), y))
            .cloned()
            .unwrap_or_default();
        let up = self
            .get((x, y.wrapping_sub(1)))
            .cloned()
            .unwrap_or_default();
        let right = self.get((x, y + 1)).cloned().unwrap_or_default();
        let bottom = self.get((x + 1, y)).cloned().unwrap_or_default();

        return (
            left.conntype << 48 | up.conntype << 32 | right.conntype << 16 | bottom.conntype,
            DirectionType::from_lurb([
                left.direction,
                up.direction,
                right.direction,
                bottom.direction,
            ]),
        );
    }

    #[inline]
    fn check_fit(&self, x: usize, y: usize, piece: Piece) -> u32 {
        let (type_mask, direction_mask) = self.get_piecemask(x, y);
        let type_care_mask = direction_mask.care_mask();

        let mut orientation_to_check = piece.direction.matches_r2(&direction_mask);
        let mut result = 0u32;
        while orientation_to_check != 0 {
            let rot = orientation_to_check.trailing_zeros();
            if piece.rotate(rot).matches(type_mask, type_care_mask) {
                result |= rot;
            }

            orientation_to_check ^= 1 << rot;
        }
        result
    }

    fn generate(types: u8, rows: usize, columns: usize, seed: u64) -> JigPuzzle {
        debug_assert!(types <= 16, "Only 16-types supported atm");
        // We use xorshiro as the RNG since thread_rng is a CSRNG which is overkill
        let mut rng = rand_xoshiro::Xoshiro128StarStar::seed_from_u64(seed);
        let mut puzzle = JigPuzzle::new_empty(rows, columns);

        // We generate a valid board using effectively conforming random sampling
        //  any existing pieces need to be matched up (including edges), anything else is random
        // This can be modeled using simple &masks and |randoms/priors
        //  in order to maintain type we use a simple galois field before assignment
        // The fastest way to do this is by filling in the bottom and right conntype
        //  and copying those over to the pieces in that direction
        //  the directionType doesn't matter as long as they are opposite for shared sides
        //  and so they can be filled in later.
        // Any additional solves can be found by deconstruct-ing the solved maze and resolving it
        //  tree-recursively in parallel with rayon or a thread-pool
        for y in 0..columns {
            for x in 0..rows {
                let mut piece = Piece {
                    conntype: 0,
                    direction: DirectionType(0),
                };

                if let Some(above) = puzzle.get((x, y.wrapping_sub(1))) {
                    // Piece above fill, inverted direction
                    piece.conntype |= (above.conntype & 0x0000_ffff) << (2 * 16);
                    piece.direction.0 |= (!above.direction.0 & 0b0000_0011) << (2 * 2);
                }
                if let Some(left) = puzzle.get((x.wrapping_sub(1), y)) {
                    // Piece left fill, inverted direction
                    piece.conntype |= (left.conntype & 0xffff_0000) << (2 * 16);
                    piece.direction.0 |= (!left.direction.0 & 0b0000_1100) << (2 * 2);
                }
                // Generate bottom and right sides
                if x < rows - 1 {
                    piece.conntype |= (rng.next_u64() % types as u64) << (1 * 16);
                    piece.direction.0 |= 1 << ((rng.next_u32() % 2) + 2);
                }
                if y < columns - 1 {
                    piece.conntype |= (rng.next_u64() % types as u64) << (0 * 16);
                    piece.direction.0 |= 1 << (rng.next_u32() % 2);
                }
                unsafe { puzzle.place_unchecked(x, y, piece) };
            }
        }
        puzzle
    }

    /// Will assert all potential ways to validate the puzzle, can be used to confirm a solving algorithm produces
    ///  valid solutions
    fn assert_validity(&self) {
        for x in 0..self.rows {
            for y in 0..self.columns {
                let point = (x, y);
                let piece = self.get(point).unwrap().clone();
                for direction in [
                    Direction::Left,
                    Direction::Right,
                    Direction::Top,
                    Direction::Bottom,
                ] {
                    let dirtype = piece.direction.unpack_dir(direction);
                    if let Some(counter_part) = self.get(direction.translate_cell(point)) {
                        assert_eq!(
                            counter_part
                                .direction
                                .unpack_dir(direction.opposite())
                                .invert(),
                            dirtype,
                            "Expected adjacent pieces to be inverse of eachother ({x}, {y}) ({direction:?}) {:?} {:?}", piece, counter_part
                        );

                        assert_eq!(
                            counter_part.unpack_type_dir(direction.opposite()),
                            piece.unpack_type_dir(direction),
                            "Expected connection type to be the same for adjacent pieces ({x}, {y}) ({direction:?})"
                        );
                    } else {
                        assert_eq!(
                            dirtype,
                            UnpackedDirectionType::Edge,
                            "No piece '{direction:?}' of us but also no edge type; something is wrong ({x}, {y})"
                        );
                        // NOTE: to save on complexity/time the direction type determines if conntype is even used
                        // assert!(
                        //     piece.unpack_type_dir(direction) == 0,
                        //     "Since it's an edge type you'd expect it's conntype to be 0"
                        // )
                    }
                }
            }
        }
    }
}

impl std::fmt::Debug for JigPuzzle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Print ascii repr of puzzle");
    }
}

use crossbeam::deque::{Injector, Steal, Worker};
use dashmap::DashSet;

type TaskDef = (Vec<Piece>, JigPuzzle, usize);

struct Solver {
    solutions: std::sync::Arc<DashSet<JigPuzzle>>,
    unclaimed_tasks: std::sync::Arc<Injector<TaskDef>>,
    threads: Vec<JoinHandle<()>>,
    explore_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    idle_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

fn _exaustive_solve(
    injector: std::sync::Arc<Injector<TaskDef>>,
    mut pieces_left: Vec<Piece>,
    mut puzzle: JigPuzzle,
    idx: usize,
) -> Option<JigPuzzle> {
    let rows = puzzle.rows;
    let columns = puzzle.columns;
    // FIXME: this needs to be refactored ideally I'd like a depth-first (i.e. we finish one puzzle before returning to the task list)
    //  however it seems pretty hard to achieve this, we'd need to build a list of possible piece placements then pop one and push the rest to the task list

    for xy in idx..(rows * columns) {
        let x = xy / columns;
        let y = xy % columns;
        let mut was_placed = false;
        for i in 0..pieces_left.len() {
            let piece = pieces_left[i];
            let mut rotations = puzzle.check_fit(x, y, piece.clone());

            while rotations.count_ones() != 0 {
                let rot = rotations.trailing_zeros();
                rotations ^= 1 << rot; // unset option

                let mut task_pieces = pieces_left.clone();
                task_pieces.swap_remove(i);
                let mut task_puzzle = puzzle.clone();
                unsafe { task_puzzle.place_unchecked(x, y, piece.rotate(rot)) };
                injector.push((task_pieces, task_puzzle, xy + 1))
            }
            // continue with last rot option
            let piece = pieces_left.swap_remove(i);
            let rot = rotations.trailing_zeros();
            unsafe { puzzle.place_unchecked(x, y, piece.rotate(rot)) };
            was_placed = true;
        }
        if !was_placed {
            // if no piece was placed this path is a dead-end so we should return None
            return None;
        }
    }
    puzzle.assert_validity();
    Some(puzzle)
}

impl Solver {
    fn new() -> Solver {
        Solver {
            solutions: std::sync::Arc::new(DashSet::new()),
            unclaimed_tasks: std::sync::Arc::new(Injector::new()),
            threads: Vec::new(),
            explore_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            idle_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn spawn_workers(&mut self, count: usize) {
        for i in 0..count {
            let inject_ref = self.unclaimed_tasks.clone();
            let solution_ref = self.solutions.clone();
            let worker_q = Worker::new_lifo();
            let explore_count_ref = self.explore_count.clone();
            let idle_count_ref = self.idle_count.clone();
            let thread_handle = std::thread::Builder::new()
                .name(std::format!("solver_thread_{i}"))
                .spawn(move || {
                    loop {
                        let steal_ref = inject_ref.clone();
                        let produce_ref = inject_ref.clone();
                        match worker_q.pop().or_else(|| {
                            // Otherwise, we need to look for a task elsewhere.
                            std::iter::repeat_with(|| {
                                // Try stealing a batch of tasks from the global queue.
                                steal_ref.steal_batch_with_limit_and_pop(&worker_q, 8)
                            })
                            // Loop while no task was stolen and any steal operation needs to be retried.
                            .find(|s| !s.is_retry())
                            // Extract the stolen task, if there is one.
                            .and_then(|s| s.success())
                        }) {
                            Some((pieces, puzzle, idx)) => {
                                explore_count_ref
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if let Some(solution) =
                                    _exaustive_solve(produce_ref, pieces, puzzle, idx)
                                {
                                    solution_ref.insert(solution);
                                }
                            }
                            // Nothing to do, yield our timeslice
                            _ => {
                                idle_count_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                std::thread::yield_now();
                                idle_count_ref.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                })
                .unwrap();
            self.threads.push(thread_handle);
        }
    }

    fn submit_work(&mut self, work: TaskDef) {
        self.unclaimed_tasks.push(work)
    }
}

fn main() {
    // Up to 16-types
    // Each piece has a 64-bit (16*4) bitflag for which type and a 4-bit bitflag for female-male
    // When checking first compare the 4-bit bitflag using a mask for which parts are known
    // To rotate the piece just rotate the bits (*16 for the connection-type)

    // To quickly eliminate non-fitting pieces you can do a simple:
    // (piece-type.unspread(4) &== type).spread(4) & (piece-bitflag ^ prev-piece-conn)
    // If this result is zero the piece has no matching type of the desired type,
    //  this can be done for each desired type (i.e. the candidate connection)

    // We start with a jigsaw with *one* known solution, this is given in generation
    //  I suspect there is a way to use this to our advantage but not sure how yet

    let puzzle = JigPuzzle::generate(7, 5, 5, 42);
    puzzle.assert_validity();
    let pieces = puzzle.deconstruct();

    // In theory we can use different aspect ratios here as long as w*h=len(pieces); although if you allow dropping pieces
    //  this doesn't necessarily even have to be true.

    let thread_count: usize = 4;
    let mut solver = Solver::new();
    solver.spawn_workers(thread_count);
    solver.submit_work((pieces, JigPuzzle::new_empty(5, 5), 0));

    // Try solving the puzzle here; keep in mind that we still need a way to differentiate between different solves.

    // FIXME: solver finds too many solutions :/ this cannot be right

    // Background threads should be running now, we can monitor their progress using the Dashmap & injector queue
    loop {
        println!(
            "\x1b[2K Running with {thread_count} threads; solutions: {}\t tasks: {}, steps: {}, idle: {}\r",
            solver.solutions.len(),
            solver.unclaimed_tasks.len(),
            solver
                .explore_count
                .load(std::sync::atomic::Ordering::Relaxed),
            solver.idle_count.load(std::sync::atomic::Ordering::Relaxed)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
