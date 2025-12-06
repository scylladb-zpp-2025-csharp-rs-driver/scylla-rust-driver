use scylla_cql::serialize::SerializationError;
use scylla_cql::serialize::row::{SerializedValues, SerializeRow};

use crate::statement::prepared::PreparedStatement;

//
// 1. Serializer traits
//

/// A serializer that exposes a borrowed view of serialized values.
/// Any necessary serialization work must have been done when the
/// serializer was created.
pub trait SerializesValuesBorrowed<T: SerializeRow> {
    fn as_serialized(&self) -> &SerializedValues;
}

/// A serializer that produces owned serialized values.
/// This is typically used when the caller wants to take ownership
/// of the buffer (e.g. to pass into a request).
pub trait SerializesValuesOwned<T: SerializeRow> {
    fn into_serialized(self) -> Result<SerializedValues, SerializationError>;
}

//
// 2. Supplier traits
//

/// A supplier that can be used multiple times without being consumed.
/// It can create a *borrowed* serializer for a given prepared statement.
///
/// For `ValuesSerializationSupplier`, this will (re-)serialize on demand.
/// For `PreSerializedSupplier`, this can simply return a borrowed view
/// of the stored `SerializedValues`.
pub trait NonConsumingSupplier<T: SerializeRow> {
    type BorrowSerializer<'p>: SerializesValuesBorrowed<T>
    where
        Self: 'p;

    fn is_empty(&self) -> bool;

    fn for_prepared_borrow<'p>(
        &'p self,
        prepared: &'p PreparedStatement,
    ) -> Result<Self::BorrowSerializer<'p>, SerializationError>
    where
        Self: 'p;
}

/// A supplier that can be *consumed* to produce an owned serializer.
/// This is useful when you have a one-shot path where you want to move
/// out the values (especially pre-serialized values) without cloning.
///
/// NOTE: The owned serializer must **not** borrow from `self`, so there
/// is intentionally **no** `Self: 'p` bound here.
pub trait ConsumingSupplier<T: SerializeRow> {
    type OwnedSerializer<'p>: SerializesValuesOwned<T>;

    fn is_empty(&self) -> bool;

    fn into_owned_serializer<'p>(
        self,
        prepared: &'p PreparedStatement,
    ) -> Result<Self::OwnedSerializer<'p>, SerializationError>;
}

//
// 3. Concrete serializers
//

/// Borrowed serializer for "normal" Rust values.
/// Internally owns a `SerializedValues` buffer created at construction.
pub struct BorrowedValueSerializer {
    values: SerializedValues,
}

impl BorrowedValueSerializer {
    #[inline(always)]
    pub fn new(values: SerializedValues) -> Self {
        Self { values }
    }
}

impl<T: SerializeRow> SerializesValuesBorrowed<T> for BorrowedValueSerializer {
    #[inline(always)]
    fn as_serialized(&self) -> &SerializedValues {
        &self.values
    }
}

/// Borrowed serializer for pre-serialized values.
/// Just wraps a `&SerializedValues`.
pub struct BorrowedPreSerializedSerializer<'p> {
    values: &'p SerializedValues,
}

impl<'p> BorrowedPreSerializedSerializer<'p> {
    #[inline(always)]
    pub fn new(values: &'p SerializedValues) -> Self {
        Self { values }
    }
}

impl<'p, T: SerializeRow> SerializesValuesBorrowed<T> for BorrowedPreSerializedSerializer<'p> {
    #[inline(always)]
    fn as_serialized(&self) -> &SerializedValues {
        self.values
    }
}

/// Owned serializer for Rust values.
/// Holds a reference to the prepared statement and owns the Rust values.
/// It serializes them when `into_serialized` is called.
pub struct OwnedValueSerializer<'p, T: SerializeRow> {
    prepared: &'p PreparedStatement,
    values:   T,
}

impl<'p, T: SerializeRow> OwnedValueSerializer<'p, T> {
    #[inline(always)]
    pub fn new(prepared: &'p PreparedStatement, values: T) -> Self {
        Self { prepared, values }
    }
}

impl<'p, T: SerializeRow> SerializesValuesOwned<T> for OwnedValueSerializer<'p, T> {
    #[inline(always)]
    fn into_serialized(self) -> Result<SerializedValues, SerializationError> {
        self.prepared.serialize_values(&self.values)
    }
}

/// Owned serializer for pre-serialized values.
/// Just moves out the `SerializedValues` without cloning.
pub struct OwnedPreSerializedSerializer {
    values: SerializedValues,
}

impl OwnedPreSerializedSerializer {
    #[inline(always)]
    pub fn new(values: SerializedValues) -> Self {
        Self { values }
    }
}

impl<T: SerializeRow> SerializesValuesOwned<T> for OwnedPreSerializedSerializer {
    #[inline(always)]
    fn into_serialized(self) -> Result<SerializedValues, SerializationError> {
        Ok(self.values)
    }
}

//
// 4. Concrete suppliers
//

/// Supplier for Rust values `T`.
/// - Non-consuming use (`NonConsumingSupplier`): re-serializes on demand
///   for a given prepared statement (used e.g. in retrying `query`).
/// - Consuming use (`ConsumingSupplier`): moves out `T` and defers
///   serialization to the owned serializer (used e.g. in `do_query_iter`).
pub struct ValuesSerializationSupplier<T: SerializeRow> {
    pub values: T,
}

impl<T: SerializeRow> ValuesSerializationSupplier<T> {
    #[inline(always)]
    pub fn new(values: T) -> Self {
        Self { values }
    }
}

impl<T: SerializeRow> NonConsumingSupplier<T> for ValuesSerializationSupplier<T> {
    type BorrowSerializer<'p> = BorrowedValueSerializer
    where
        Self: 'p;

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline(always)]
    fn for_prepared_borrow<'p>(
        &'p self,
        prepared: &'p PreparedStatement,
    ) -> Result<Self::BorrowSerializer<'p>, SerializationError>
    where
        Self: 'p,
    {
        // Non-consuming path: serialize immediately for this prepared statement.
        let values = prepared.serialize_values(&self.values)?;
        Ok(BorrowedValueSerializer::new(values))
    }
}

impl<T: SerializeRow> ConsumingSupplier<T> for ValuesSerializationSupplier<T> {
    type OwnedSerializer<'p> = OwnedValueSerializer<'p, T>;

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline(always)]
    fn into_owned_serializer<'p>(
        self,
        prepared: &'p PreparedStatement,
    ) -> Result<Self::OwnedSerializer<'p>, SerializationError> {
        // Consuming path: move out T; actual serialization is deferred
        // to OwnedValueSerializer::into_serialized.
        Ok(OwnedValueSerializer::new(prepared, self.values))
    }
}

/// Supplier for pre-serialized values.
/// - Non-consuming: returns a borrowed view of the stored values.
/// - Consuming: moves out the stored `SerializedValues` without cloning.
pub struct PreSerializedSupplier {
    pub values: SerializedValues,
}

impl PreSerializedSupplier {
    #[inline(always)]
    pub fn new(values: SerializedValues) -> Self {
        Self { values }
    }
}

impl<T: SerializeRow> NonConsumingSupplier<T> for PreSerializedSupplier {
    type BorrowSerializer<'p> = BorrowedPreSerializedSerializer<'p>
    where
        Self: 'p;

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline(always)]
    fn for_prepared_borrow<'p>(
        &'p self,
        _prepared: &'p PreparedStatement,
    ) -> Result<Self::BorrowSerializer<'p>, SerializationError>
    where
        Self: 'p,
    {
        // No serialization needed; just borrow the stored values.
        Ok(BorrowedPreSerializedSerializer::new(&self.values))
    }
}

impl<T: SerializeRow> ConsumingSupplier<T> for PreSerializedSupplier {
    type OwnedSerializer<'p> = OwnedPreSerializedSerializer;

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline(always)]
    fn into_owned_serializer<'p>(
        self,
        _prepared: &'p PreparedStatement,
    ) -> Result<Self::OwnedSerializer<'p>, SerializationError> {
        // Move out the pre-serialized values without cloning.
        Ok(OwnedPreSerializedSerializer::new(self.values))
    }
}
