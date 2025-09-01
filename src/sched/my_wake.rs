use std::rc::Rc;
use std::task::{RawWaker, RawWakerVTable, Waker};

// Define your own wake trait for Rc
pub trait LocalWake {
    fn wake(self: Rc<Self>);
    fn wake_by_ref(self: &Rc<Self>);
}

pub fn my_waker_create<T>(rc: Rc<T>) -> Waker
where
    T: LocalWake + 'static,
{
    unsafe fn clone<T: LocalWake>(data: *const ()) -> RawWaker {
        let rc = Rc::from_raw(data as *const T);
        let cloned = rc.clone();
        std::mem::forget(rc); // prevent drop
        RawWaker::new(Rc::into_raw(cloned) as *const (), vtable::<T>())
    }

    unsafe fn wake<T: LocalWake>(data: *const ()) {
        let rc = Rc::from_raw(data as *const T);
        LocalWake::wake(rc);
        // rc consumed
    }

    unsafe fn wake_by_ref<T: LocalWake>(data: *const ()) {
        let rc = Rc::from_raw(data as *const T);
        LocalWake::wake_by_ref(&rc);
        std::mem::forget(rc); // keep ownership
    }

    unsafe fn drop<T: LocalWake>(data: *const ()) {
        let rc = Rc::from_raw(data as *const T);
        std::mem::drop(rc);
    }

    const fn vtable<T: LocalWake>() -> &'static RawWakerVTable {
        &RawWakerVTable::new(
            clone::<T>,
            wake::<T>,
            wake_by_ref::<T>,
            drop::<T>,
        )
    }

    let raw = RawWaker::new(Rc::into_raw(rc) as *const (), vtable::<T>());
    unsafe { Waker::from_raw(raw) }
}


// pub(crate) unsafe fn my_waker_extract_rc<T>(waker: &Waker) -> Rc<T> {
//     let ptr = waker.data();
//     let rc = Rc::from_raw(ptr as *const T);
//     let cloned = rc.clone();
//     std::mem::forget(rc);
//     cloned
// }

// 
// pub(crate) unsafe fn my_waker_extract_rc<T>(waker: &Waker) -> Rc<T> {
//     let raw = waker.clone();
//     let ptr = raw.data();
//     Rc::from_raw(ptr as *const T)
// }
// 
// pub(crate) unsafe fn my_waker_extract_rc_cloned<T>(waker: &Waker) -> Rc<T> {
//     let raw = waker.clone();
//     let ptr = raw.data();
//     let rc = Rc::from_raw(ptr as *const T);
//     let cloned = rc.clone();
//     std::mem::forget(rc);
//     cloned
// }